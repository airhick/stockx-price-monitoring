"""
StockX Price Monitor — curl_cffi edition.

No browser window, no Chromium process. Uses curl_cffi to impersonate Chrome at
the TLS/HTTP2 fingerprint level. The session visits stockx.com once on startup
(and every COOKIE_TTL seconds) to obtain a valid cf_clearance cookie inside the
same TLS context — so Cloudflare's fingerprint binding is satisfied natively.
"""
import asyncio
import json
import os
import sqlite3
import threading
import time
import functools
from collections import deque
from pathlib import Path

import aiohttp
from aiohttp import web
from curl_cffi.requests import AsyncSession

print = functools.partial(print, flush=True)

# ── CONFIG (env-overridable) ──────────────────────────────────────────────────
SLUGS_FILE      = os.environ.get("STOCKX_SLUGS",  "stockx_slugs.txt")
CACHE_FILE      = os.environ.get("STOCKX_CACHE",  "price_cache.json")
WEBHOOK_URL     = os.environ.get("STOCKX_WEBHOOK", "")  # empty = log only
CURRENCY        = os.environ.get("STOCKX_CURRENCY", "EUR")
MARKET          = os.environ.get("STOCKX_MARKET",   "FR")
COUNTRY         = os.environ.get("STOCKX_COUNTRY",  "FR")
POLL_INTERVAL   = int(os.environ.get("STOCKX_INTERVAL",   "15"))   # seconds between chunks
CONCURRENCY     = int(os.environ.get("STOCKX_CONCURRENCY", "40"))  # parallel HTTP requests
CHUNK_SIZE      = int(os.environ.get("STOCKX_CHUNK", "500"))       # slugs per batch
COOKIE_REFRESH  = int(os.environ.get("STOCKX_COOKIE_TTL", "1800"))
UI_PORT         = int(os.environ.get("STOCKX_UI_PORT", "8787"))
CATALOG_FILE    = os.environ.get("STOCKX_CATALOG", "stockx_catalog.json")
DB_FILE         = os.environ.get("STOCKX_DB",      "price_history.db")

GQL_URL = "https://gateway.stockx.com/api/graphql"
CLIENT_VERSION = "2026.04.16.00"

QUERY = """
query GetCheckoutProduct($id: String!, $currencyCode: CurrencyCode, $country: String!, $market: String) {
  product(id: $id) {
    id
    title
    variants {
      hidden
      id
      traits { size }
      market(currencyCode: $currencyCode) {
        state(country: $country, market: $market) {
          askServiceLevels { standard { lowest { amount } } }
          bidInventoryTypes { standard { highest { amount } } }
        }
        salesInformation { lastSale }
      }
    }
  }
}
"""

# ── HTTP FETCHER (curl_cffi — no browser) ─────────────────────────────────────
_GQL_HEADERS = {
    "content-type":                   "application/json",
    "apollographql-client-name":      "Iron",
    "apollographql-client-version":   CLIENT_VERSION,
    "app-platform":                   "Iron",
    "x-operation-name":               "GetCheckoutProduct",
    "accept":                         "*/*",
    "origin":                         "https://stockx.com",
    "referer":                        "https://stockx.com/",
    "accept-language":                "en-US,en;q=0.9",
}


class StockXFetcher:
    def __init__(self):
        self._session: AsyncSession | None = None
        self._sem = asyncio.Semaphore(CONCURRENCY)
        self._last_refresh = 0.0

    async def start(self):
        self._session = AsyncSession(impersonate="chrome131")
        print("[SETUP] curl_cffi session started (no browser)")
        await self._warmup()

    async def _warmup(self):
        print("[WARMUP] Visiting stockx.com to refresh Cloudflare cookies...")
        try:
            r = await self._session.get(
                "https://stockx.com/",
                timeout=30,
                headers={"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                         "accept-language": "en-US,en;q=0.9"},
            )
            has_cf = "cf_clearance" in r.cookies
            print(f"[WARMUP] HTTP {r.status_code} — cf_clearance={'OK' if has_cf else 'MISSING'}")
        except Exception as e:
            print(f"[WARN] warmup failed: {e}")
        self._last_refresh = time.time()

    async def maybe_refresh(self):
        if time.time() - self._last_refresh > COOKIE_REFRESH:
            await self._warmup()

    async def hard_reset(self):
        await self._warmup()

    async def light_reset(self):
        pass

    async def fetch_batch(self, slugs: list[str], _retries: int = 3) -> list[dict | None]:
        t0 = time.time()
        tasks = [self._fetch_one(s) for s in slugs]
        results: list[dict | None] = list(await asyncio.gather(*tasks))
        ok = sum(1 for r in results if r is not None)
        blocked = len(slugs) - ok
        elapsed = time.time() - t0
        print(f"    [fetch_batch] {ok}/{len(slugs)} OK  {blocked} failed  {elapsed:.1f}s")
        if blocked > len(slugs) // 2:
            print(f"    [WARN] majority blocked — refreshing session")
            await self._warmup()
        return results

    async def _fetch_one(self, slug: str) -> dict | None:
        body = {
            "operationName": "GetCheckoutProduct",
            "variables": {"id": slug, "country": COUNTRY, "market": MARKET, "currencyCode": CURRENCY},
            "query": QUERY,
        }
        async with self._sem:
            for attempt in range(3):
                try:
                    r = await self._session.post(GQL_URL, json=body,
                                                 headers=_GQL_HEADERS, timeout=20)
                    if r.status_code == 200:
                        return r.json()
                    if r.status_code in (429, 503) and attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return None
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(1)
                    continue
        return None

    async def stop(self):
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass


# ── PARSING ───────────────────────────────────────────────────────────────────
def parse_product(data: dict):
    product = (data.get("data") or {}).get("product") or {}
    pid = product.get("id")
    title = product.get("title")
    variants, prices = [], {}
    for v in (product.get("variants") or []):
        if v.get("hidden"):
            continue
        size = ((v.get("traits") or {}).get("size") or "")
        m = v.get("market") or {}
        st = m.get("state") or {}
        info = m.get("salesInformation") or {}
        ask = ((st.get("askServiceLevels") or {}).get("standard") or {}).get("lowest") or {}
        bid = ((st.get("bidInventoryTypes") or {}).get("standard") or {}).get("highest") or {}
        variants.append({"id": v["id"], "size": size})
        prices[size] = {
            "lowestAsk":  ask.get("amount"),
            "highestBid": bid.get("amount"),
            "lastSale":   info.get("lastSale"),
        }
    return pid, title, variants, prices


# ── CACHE ─────────────────────────────────────────────────────────────────────
def load_cache() -> dict:
    p = Path(CACHE_FILE)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            print(f"[WARN] cache unreadable, starting fresh")
    return {}

def save_cache(cache: dict):
    tmp = Path(CACHE_FILE).with_suffix(".tmp")
    tmp.write_text(json.dumps(cache))
    tmp.replace(CACHE_FILE)


# ── PRICE HISTORY DB ─────────────────────────────────────────────────────────
_db_conn: sqlite3.Connection | None = None
_db_lock = threading.Lock()

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            slug      TEXT    NOT NULL,
            size      TEXT    NOT NULL,
            ts        INTEGER NOT NULL,
            ask       REAL,
            bid       REAL,
            last_sale REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_slug_size_ts
        ON price_history(slug, size, ts)
    """)
    conn.commit()
    print(f"[DB] price_history opened: {DB_FILE}")
    return conn

def db_write(rows: list[tuple]):
    """rows: list of (slug, size, ts, ask, bid, last_sale)."""
    if not _db_conn or not rows:
        return
    with _db_lock:
        _db_conn.executemany(
            "INSERT INTO price_history(slug,size,ts,ask,bid,last_sale) VALUES(?,?,?,?,?,?)",
            rows,
        )
        _db_conn.commit()

def db_query_history(slug: str) -> dict:
    """Returns {size: [{ts, ask, bid, lastSale}]} sorted by ts asc."""
    if not _db_conn:
        return {}
    with _db_lock:
        cur = _db_conn.execute(
            "SELECT size,ts,ask,bid,last_sale FROM price_history WHERE slug=? ORDER BY size,ts",
            (slug,),
        )
        rows = cur.fetchall()
    result: dict[str, list] = {}
    for size, ts, ask, bid, ls in rows:
        result.setdefault(size, []).append({"ts": ts, "ask": ask, "bid": bid, "lastSale": ls})
    return result


# ── WEBHOOK ───────────────────────────────────────────────────────────────────
def _size_sort_key(size):
    s = str(size)
    num = ""
    for ch in s:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            break
    try:
        return (float(num) if num else float("inf"), s)
    except ValueError:
        return (float("inf"), s)

def _pair(old, new):
    a = old if old is not None else "—"
    b = new if new is not None else "—"
    return f"{a}→{b}"

def _pct(old, new):
    try:
        if old and new and old != new:
            return f"{(new-old)/old*100:+.1f}%"
    except Exception:
        pass
    return ""

def _pretty_slug(slug):
    return slug.replace("-", " ").title()

def _build_slug_table(record):
    """Returns (description, net_ask_delta) for one slug record with all sizes."""
    sizes = sorted(record["sizes"], key=lambda s: _size_sort_key(s["size"]))

    up = down = 0
    net = 0
    for s in sizes:
        if not s["changed"]:
            continue
        o, n = s["old"].get("lowestAsk"), s["new"].get("lowestAsk")
        if o is not None and n is not None:
            if n > o: up += 1
            elif n < o: down += 1
            net += (n - o)

    raw = []
    for s in sizes:
        ask_new = s["new"].get("lowestAsk")
        bid_new = s["new"].get("highestBid")
        if s["changed"]:
            ask_cell = _pair(s["old"].get("lowestAsk"),  ask_new)
            bid_cell = _pair(s["old"].get("highestBid"), bid_new)
            pct      = _pct(s["old"].get("lowestAsk"),   ask_new)
            marker   = "•"
        else:
            ask_cell = str(ask_new) if ask_new is not None else "—"
            bid_cell = str(bid_new) if bid_new is not None else "—"
            pct      = ""
            marker   = " "
        raw.append((marker, str(s["size"]), ask_cell, pct, bid_cell))

    w_size = max(len(r[1]) for r in raw)
    w_ask  = max(len(r[2]) for r in raw)
    w_pct  = max((len(r[3]) for r in raw), default=0)
    w_bid  = max(len(r[4]) for r in raw)

    lines = [
        f"{m} {sz.ljust(w_size)}  ask {a.ljust(w_ask)}  {p.ljust(w_pct)}  bid {b.ljust(w_bid)}"
        for (m, sz, a, p, b) in raw
    ]
    table = "```\n" + "\n".join(lines) + "\n```"

    changed_count = sum(1 for s in sizes if s["changed"])
    total = len(sizes)
    tag = []
    if up:   tag.append(f"↑{up}")
    if down: tag.append(f"↓{down}")
    tag_str = "  " + " ".join(tag) if tag else ""
    header = f"**{changed_count}/{total} size{'s' if total > 1 else ''} changed**{tag_str}"

    description = f"{header}\n{table}"
    if len(description) > 900:
        description = description[:890] + "\n…```"

    return description, net

def _build_revolt_embed(record):
    description, net = _build_slug_table(record)
    if   net > 0: colour = "#2ecc71"
    elif net < 0: colour = "#e74c3c"
    else:         colour = "#95a5a6"
    return {
        "title": _pretty_slug(record["slug"]),
        "url":   record["url"],
        "description": description,
        "colour": colour,
    }

def _build_discord_embed(record):
    description, net = _build_slug_table(record)
    if   net > 0: color = 0x2ecc71
    elif net < 0: color = 0xe74c3c
    else:         color = 0x95a5a6
    return {
        "title": _pretty_slug(record["slug"]),
        "url":   record["url"],
        "color": color,
        "description": description,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record["timestamp"])),
    }

def _pack_revolt(records):
    """One embed per message — stoat.chat enforces a small per-message payload cap."""
    return [{"embeds": [_build_revolt_embed(r)]} for r in records]

def _payload_for(url: str, changes: list[dict]) -> list[dict]:
    """Returns a list of HTTP payloads to POST. Discord = batched embeds (10/msg max)."""
    if "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url:
        embeds = [_build_discord_embed(c) for c in changes]
        return [{"embeds": embeds[i:i+10]} for i in range(0, len(embeds), 10)]
    if "stoat.chat/api/webhooks" in url or "revolt.chat/api/webhooks" in url:
        return _pack_revolt(changes)
    # generic webhook: one POST per change, raw payload
    return changes

async def dispatch_changes(http, changes):
    if not WEBHOOK_URL or WEBHOOK_URL.startswith("https://YOUR_") or not changes:
        return
    payloads = _payload_for(WEBHOOK_URL, changes)
    sent = fail = 0
    for p in payloads:
        try:
            async with http.post(WEBHOOK_URL, json=p, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if 200 <= r.status < 300:
                    sent += 1
                else:
                    fail += 1
                    body = (await r.text())[:200]
                    print(f"[WEBHOOK] HTTP {r.status}: {body}")
        except Exception as e:
            fail += 1
            print(f"[WEBHOOK] error: {e}")
    print(f"[WEBHOOK] sent={sent} fail={fail} (changes={len(changes)})")


# ── LOCAL UI ──────────────────────────────────────────────────────────────────
_cache_ref = {"data": {}}
_master_slugs: list[str] = []
_subscribers: list[asyncio.Queue] = []
_image_map: dict[str, str] = {}  # slug -> image_url from catalog

def load_image_map() -> dict[str, str]:
    p = Path(CATALOG_FILE)
    if not p.exists():
        print(f"[WARN] catalog file not found: {CATALOG_FILE} — no images")
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        mapping = {item["slug"]: item["image_url"]
                   for item in data.get("products", [])
                   if item.get("slug") and item.get("image_url")}
        print(f"[OK] image map: {len(mapping)} entries from {CATALOG_FILE}")
        return mapping
    except Exception as e:
        print(f"[WARN] could not load image map: {e}")
        return {}

def _summary(slug):
    entry = _cache_ref["data"].get(slug, {})
    prices = entry.get("prices", {})
    asks = [p["lowestAsk"]  for p in prices.values() if p.get("lowestAsk")  is not None]
    bids = [p["highestBid"] for p in prices.values() if p.get("highestBid") is not None]
    return {
        "slug":     slug,
        "id":       entry.get("id"),
        "title":    entry.get("title"),
        "sizes":    len(entry.get("variants", [])),
        "ask":      min(asks) if asks else None,
        "bid":      max(bids) if bids else None,
        "imageUrl": _image_map.get(slug),
    }

def _notify(event):
    dead = []
    for q in _subscribers:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try: _subscribers.remove(q)
        except ValueError: pass

INDEX_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>StockX Monitor</title>
<style>
  :root { --bg:#0d1117; --panel:#161b22; --panel2:#1f242c; --border:#30363d; --text:#c9d1d9; --muted:#8b949e; --accent:#58a6ff; --up:#2ea043; --down:#f85149; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: var(--bg); color: var(--text); font-size: 13px; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
  header { padding: 10px 16px; border-bottom: 1px solid var(--border); background: var(--bg); display: flex; gap: 12px; align-items: center; flex: 0 0 auto; }
  header h1 { margin: 0; font-size: 14px; font-weight: 500; color: var(--muted); white-space: nowrap; }
  header #total { font-size: 12px; color: var(--muted); white-space: nowrap; }
  header #status { font-size: 12px; color: var(--muted); margin-left: 8px; white-space: nowrap; }
  header #status.live::before { content: "●"; color: var(--up); margin-right: 4px; }
  header #status.dead::before { content: "●"; color: var(--down); margin-right: 4px; }
  input { flex: 1; padding: 7px 10px; font-size: 13px; background: var(--panel); color: var(--text); border: 1px solid var(--border); border-radius: 6px; outline: none; }
  input:focus { border-color: var(--accent); }
  main { flex: 1; display: flex; flex-direction: column; min-height: 0; }
  #cards { padding: 10px 16px 0; max-height: 40vh; overflow-y: auto; flex: 0 0 auto; }
  .card { background: var(--panel); border: 1px solid var(--border); border-radius: 6px; margin-bottom: 8px; padding: 10px 12px; }
  .card-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; gap: 8px; }
  .card-head .ttl { color: var(--text); font-weight: 500; font-size: 13px; }
  .card-head .meta { color: var(--muted); font-size: 11px; font-family: ui-monospace, Menlo, monospace; }
  .card-head a { color: var(--accent); text-decoration: none; font-size: 11px; font-family: ui-monospace, Menlo, monospace; word-break: break-all; }
  .card-head a:hover { text-decoration: underline; }
  .close { cursor: pointer; color: var(--muted); font-size: 18px; padding: 0 6px; user-select: none; }
  .close:hover { color: var(--text); }
  .card table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
  .card th, .card td { text-align: left; padding: 3px 10px 3px 0; font-size: 12px; }
  .card th { color: var(--muted); font-weight: normal; border-bottom: 1px solid var(--border); }
  @keyframes flashup { 0% { background: rgba(46,160,67,0.4); } 100% { background: transparent; } }
  @keyframes flashdown { 0% { background: rgba(248,81,73,0.4); } 100% { background: transparent; } }
  tr.up, .row.up { animation: flashup 1.5s ease-out; }
  tr.down, .row.down { animation: flashdown 1.5s ease-out; }

  #list { flex: 1; min-height: 0; display: flex; flex-direction: column; border-top: 1px solid var(--border); }
  #list-head { display: grid; grid-template-columns: 32px 1fr 220px 70px 90px 90px; gap: 8px; padding: 6px 16px; background: var(--panel); color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid var(--border); flex: 0 0 auto; }
  #list-scroll { flex: 1; overflow-y: auto; position: relative; }
  #list-spacer { position: relative; width: 100%; }
  #list-rows { position: absolute; top: 0; left: 0; right: 0; }
  .row { position: absolute; left: 0; right: 0; display: grid; grid-template-columns: 32px 1fr 220px 70px 90px 90px; gap: 8px; padding: 6px 16px; height: 32px; align-items: center; border-bottom: 1px solid var(--border); cursor: pointer; font-size: 12px; }
  .row:hover { background: var(--panel2); }
  .thumb { width: 32px; height: 24px; object-fit: contain; border-radius: 2px; }
  .card-img { width: 60px; height: 45px; object-fit: contain; margin-left: 12px; border-radius: 4px; background: #fff; }
  .card-head-left { flex: 1; min-width: 0; }
  .row .slug { font-family: ui-monospace, Menlo, monospace; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .row .id { font-family: ui-monospace, Menlo, monospace; color: var(--muted); font-size: 11px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .row .sizes, .row .ask, .row .bid { font-variant-numeric: tabular-nums; text-align: right; }
  .row .ask { color: #79c0ff; }
  .row .bid { color: #d2a8ff; }
  .row .skel { color: #444; font-style: italic; }
  /* tabs */
  .ctabs { display: flex; gap: 4px; margin: 6px 0 8px; }
  .ctab { background: none; border: 1px solid var(--border); color: var(--muted); font-size: 11px; padding: 3px 10px; border-radius: 4px; cursor: pointer; }
  .ctab.active { background: var(--accent); border-color: var(--accent); color: #fff; }
  .ctab:hover:not(.active) { border-color: var(--accent); color: var(--accent); }
  .cpanel.hidden { display: none; }
  .chart-wrap { position: relative; height: 220px; width: 100%; }
  .chart-msg { color: var(--muted); font-size: 12px; padding: 8px 0; }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
</head>
<body>
<header>
  <h1>StockX Monitor</h1>
  <input id="q" type="search" placeholder="Search by name, slug, or StockX URL…" autocomplete="off">
  <span id="total">…</span>
  <span id="status" class="dead">offline</span>
</header>
<main>
  <div id="cards"></div>
  <div id="list">
    <div id="list-head"><div>Img</div><div>Slug</div><div>Product ID</div><div style="text-align:right">Sizes</div><div style="text-align:right">Min ask</div><div style="text-align:right">Max bid</div></div>
    <div id="list-scroll">
      <div id="list-spacer"><div id="list-rows"></div></div>
    </div>
  </div>
</main>
<script>
const ROW_H = 32, BATCH = 200, CACHE_CAP = 80;  // cap ~16k rows in JS heap
const cardsEl = document.getElementById("cards");
const status = document.getElementById("status");
const totalEl = document.getElementById("total");
const scrollEl = document.getElementById("list-scroll");
const spacer = document.getElementById("list-spacer");
const rowsEl = document.getElementById("list-rows");
const q = document.getElementById("q");

const pinned = new Map();
const batchCache = new Map();   // offset -> array
const batchOrder = [];          // LRU order
const pending = new Set();
let total = 0;
let filterMode = false;
let filtered = [];              // active filter results

function fmt(v) { return (v === null || v === undefined) ? "—" : v; }
function sizeKey(s) { const m = String(s).match(/^(\\d+(?:\\.\\d+)?)/); return m ? parseFloat(m[1]) : 1e9; }
function escAttr(s) { return String(s).replace(/[&"<>]/g, c => ({'&':'&amp;','"':'&quot;','<':'&lt;','>':'&gt;'}[c])); }

function rememberBatch(off, data) {
  batchCache.set(off, data);
  const idx = batchOrder.indexOf(off);
  if (idx >= 0) batchOrder.splice(idx, 1);
  batchOrder.push(off);
  while (batchOrder.length > CACHE_CAP) {
    const old = batchOrder.shift();
    batchCache.delete(old);
  }
}

async function fetchBatch(offset) {
  if (batchCache.has(offset) || pending.has(offset)) return;
  pending.add(offset);
  try {
    const r = await fetch(`/api/list?offset=${offset}&limit=${BATCH}`);
    const d = await r.json();
    rememberBatch(offset, d);
  } catch {}
  finally {
    pending.delete(offset);
    render();
  }
}

function rowHTML(i, s) {
  const slug = escAttr(s.slug);
  const id = s.id ? escAttr(s.id) : '<span class="skel">—</span>';
  const img = s.imageUrl ? `<img src="${escAttr(s.imageUrl)}" class="thumb" loading="lazy">` : `<span></span>`;
  return `<div class="row" style="top:${i*ROW_H}px" data-slug="${slug}">
    ${img}
    <span class="slug" title="${slug}">${slug}</span>
    <span class="id">${id}</span>
    <span class="sizes">${s.sizes || 0}</span>
    <span class="ask">${fmt(s.ask)}</span>
    <span class="bid">${fmt(s.bid)}</span>
  </div>`;
}

function getItem(i) {
  if (filterMode) return filtered[i];
  const off = Math.floor(i / BATCH) * BATCH;
  const b = batchCache.get(off);
  return b ? b[i - off] : null;
}

function ensureLoaded(first, last) {
  if (filterMode) return;
  const startBatch = Math.floor(first / BATCH) * BATCH;
  const endBatch   = Math.ceil(last  / BATCH) * BATCH;
  for (let off = startBatch; off < endBatch; off += BATCH) fetchBatch(off);
}

function render() {
  const top = scrollEl.scrollTop;
  const h = scrollEl.clientHeight;
  const count = filterMode ? filtered.length : total;
  spacer.style.height = (count * ROW_H) + "px";
  const first = Math.max(0, Math.floor(top / ROW_H) - 4);
  const last  = Math.min(count, Math.ceil((top + h) / ROW_H) + 4);
  ensureLoaded(first, last);

  const out = [];
  for (let i = first; i < last; i++) {
    const it = getItem(i);
    if (!it) {
      out.push(`<div class="row" style="top:${i*ROW_H}px"><span></span><span class="slug skel">loading…</span><span></span><span></span><span></span><span></span></div>`);
    } else {
      out.push(rowHTML(i, it));
    }
  }
  rowsEl.innerHTML = out.join("");
}

scrollEl.addEventListener("scroll", render, {passive: true});
window.addEventListener("resize", render);

rowsEl.addEventListener("click", e => {
  const row = e.target.closest(".row[data-slug]");
  if (row) openCard(row.dataset.slug);
});

async function openCard(slug) {
  if (pinned.has(slug)) { pinned.get(slug).scrollIntoView({behavior:"smooth", block:"nearest"}); return; }
  const r = await fetch("/api/item/" + encodeURIComponent(slug));
  if (!r.ok) return;
  const d = await r.json();
  const card = document.createElement("div");
  card.className = "card";
  card.dataset.slug = slug;
  const ttl = d.title ? escAttr(d.title) : escAttr(slug);
  const imgHtml = d.imageUrl ? `<img src="${escAttr(d.imageUrl)}" class="card-img" alt="">` : ``;
  const cid = "ch-" + slug.replace(/[^a-z0-9]/g,"-");
  card.innerHTML = `
    <div class="card-head">
      <div class="card-head-left">
        <div class="ttl">${ttl}</div>
        <a href="https://stockx.com/${slug}" target="_blank" rel="noopener">${escAttr(slug)}</a>
        <span class="meta">${d.id ? escAttr(d.id) : ""}</span>
      </div>
      <div style="display:flex; align-items:center;">
        ${imgHtml}
        <span class="close" title="close">×</span>
      </div>
    </div>
    <div class="ctabs">
      <button class="ctab active" data-t="table">Table</button>
      <button class="ctab" data-t="chart">Chart ▸</button>
    </div>
    <div class="cpanel" data-p="table">
      <table><thead><tr><th>Size</th><th>Ask</th><th>Bid</th><th>Last sale</th></tr></thead><tbody></tbody></table>
    </div>
    <div class="cpanel hidden" data-p="chart">
      <div id="${cid}-wrap" class="chart-wrap"><p class="chart-msg">Click “Chart” to load history…</p></div>
    </div>
  `;
  card.querySelector(".close").onclick = () => { card.remove(); pinned.delete(slug); };
  const tbody = card.querySelector("tbody");
  const sorted = Object.entries(d.prices || {}).sort((a, b) => sizeKey(a[0]) - sizeKey(b[0]));
  if (!sorted.length) tbody.innerHTML = `<tr><td colspan="4" class="skel">no variants yet</td></tr>`;
  for (const [size, p] of sorted) {
    const tr = document.createElement("tr");
    tr.dataset.size = size;
    tr.innerHTML = `<td>${escAttr(size)}</td><td class="ask">${fmt(p.lowestAsk)}</td><td class="bid">${fmt(p.highestBid)}</td><td class="last">${fmt(p.lastSale)}</td>`;
    tbody.appendChild(tr);
  }
  let chartLoaded = false;
  card.querySelectorAll(".ctab").forEach(btn => {
    btn.onclick = () => {
      card.querySelectorAll(".ctab").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      const t = btn.dataset.t;
      card.querySelectorAll(".cpanel").forEach(p => p.classList.toggle("hidden", p.dataset.p !== t));
      if (t === "chart" && !chartLoaded) { chartLoaded = true; loadChart(slug, cid); }
    };
  });
  cardsEl.prepend(card);
  pinned.set(slug, card);
}

const _PALETTE = ["#58a6ff","#f78166","#3fb950","#d2a8ff","#ffa657","#79c0ff","#ff7b72","#56d364","#bc8cff","#e3b341"];

async function loadChart(slug, cid) {
  const wrap = document.getElementById(cid + "-wrap");
  if (!wrap) return;
  wrap.innerHTML = '<p class="chart-msg">Loading…</p>';
  let hist;
  try {
    const r = await fetch("/api/history/" + encodeURIComponent(slug));
    hist = await r.json();
  } catch(e) {
    wrap.innerHTML = '<p class="chart-msg">Failed to load history.</p>';
    return;
  }
  const sizes = Object.keys(hist);
  if (!sizes.length) {
    wrap.innerHTML = '<p class="chart-msg">No history yet — prices are recorded on first-seen and on every change.</p>';
    return;
  }
  const datasets = sizes.map((sz, i) => ({
    label: sz,
    data: hist[sz].map(p => ({ x: p.ts * 1000, y: p.ask })),
    borderColor: _PALETTE[i % _PALETTE.length],
    backgroundColor: _PALETTE[i % _PALETTE.length] + "33",
    borderWidth: 1.5,
    pointRadius: hist[sz].length > 60 ? 0 : 3,
    stepped: "before",
    tension: 0,
  }));
  const canvas = document.createElement("canvas");
  canvas.id = cid;
  wrap.innerHTML = "";
  wrap.appendChild(canvas);
  new Chart(canvas, {
    type: "line",
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position:"bottom", labels:{ color:"#c9d1d9", boxWidth:10, font:{size:10} } },
        tooltip: {
          backgroundColor:"#161b22", borderColor:"#30363d", borderWidth:1,
          titleColor:"#c9d1d9", bodyColor:"#8b949e",
          callbacks: {
            title: items => new Date(items[0].parsed.x).toLocaleString(),
            label: item => ` ${item.dataset.label}: ${item.parsed.y ?? "—"}`,
          }
        }
      },
      scales: {
        x: { type:"time", ticks:{ color:"#8b949e", maxTicksLimit:6 }, grid:{ color:"#30363d" } },
        y: { ticks:{ color:"#8b949e" }, grid:{ color:"#30363d" }, title:{ display:true, text:"Ask price", color:"#8b949e" } }
      }
    }
  });
}

function flashRow(slug, dir) {
  const r = rowsEl.querySelector(`.row[data-slug="${CSS.escape(slug)}"]`);
  if (!r) return;
  r.classList.remove("up", "down");
  void r.offsetWidth;
  r.classList.add(dir);
}

function applyUpdate(ev) {
  // Update pinned card row if present
  const card = pinned.get(ev.slug);
  if (card) {
    const tr = card.querySelector(`tr[data-size="${CSS.escape(ev.size)}"]`);
    if (tr) {
      const askCell = tr.querySelector(".ask");
      const bidCell = tr.querySelector(".bid");
      const oldAsk = parseFloat(askCell.textContent);
      askCell.textContent = fmt(ev.new.ask);
      bidCell.textContent = fmt(ev.new.bid);
      const newAsk = parseFloat(askCell.textContent);
      tr.classList.remove("up", "down");
      void tr.offsetWidth;
      tr.classList.add((!isNaN(oldAsk) && !isNaN(newAsk) && newAsk < oldAsk) ? "down" : "up");
    } else {
      // size not in card yet: re-fetch detail
      reloadCard(ev.slug);
    }
  }
  // Flash list row
  const dir = (ev.new.ask !== null && ev.old.ask !== null && ev.new.ask < ev.old.ask) ? "down" : "up";
  flashRow(ev.slug, dir);
}

async function reloadCard(slug) {
  const card = pinned.get(slug);
  if (!card) return;
  const r = await fetch("/api/item/" + encodeURIComponent(slug));
  if (!r.ok) return;
  const d = await r.json();
  const tbody = card.querySelector("tbody");
  tbody.innerHTML = "";
  const sorted = Object.entries(d.prices || {}).sort((a, b) => sizeKey(a[0]) - sizeKey(b[0]));
  for (const [size, p] of sorted) {
    const tr = document.createElement("tr");
    tr.dataset.size = size;
    tr.innerHTML = `<td>${escAttr(size)}</td><td class="ask">${fmt(p.lowestAsk)}</td><td class="bid">${fmt(p.highestBid)}</td><td class="last">${fmt(p.lastSale)}</td>`;
    tbody.appendChild(tr);
  }
}

// Filter (server-side substring search)
let filterTimer = null;
q.addEventListener("input", () => { clearTimeout(filterTimer); filterTimer = setTimeout(applyFilter, 200); });
function normalizeQuery(raw) {
  let v = raw.trim().toLowerCase();
  // Strip StockX URL prefix
  for (const pfx of ['https://stockx.com/', 'http://stockx.com/', 'stockx.com/']) {
    if (v.startsWith(pfx)) { v = v.slice(pfx.length); break; }
  }
  v = v.split('?')[0].split('#')[0].replace(/\/+$/, '');
  // Spaces to dashes
  v = v.replace(/ /g, '-');
  return v;
}
async function applyFilter() {
  const v = normalizeQuery(q.value);
  if (v.length < 2) { filterMode = false; filtered = []; scrollEl.scrollTop = 0; render(); return; }
  const r = await fetch(`/api/search?q=${encodeURIComponent(v)}&limit=200`);
  const slugs = await r.json();
  const detail = await fetch(`/api/summaries`, {method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({slugs})}).then(r => r.json());
  filterMode = true;
  filtered = detail;
  scrollEl.scrollTop = 0;
  render();
}

// Boot
async function init() {
  const r = await fetch("/api/count");
  const d = await r.json();
  total = d.total;
  totalEl.textContent = `${total.toLocaleString()} items`;
  render();
}
init();

// SSE
function connect() {
  const es = new EventSource("/api/stream");
  es.onopen = () => { status.className = "live"; status.textContent = "live"; };
  es.onerror = () => { status.className = "dead"; status.textContent = "reconnecting…"; es.close(); setTimeout(connect, 2000); };
  es.onmessage = e => { try { applyUpdate(JSON.parse(e.data)); } catch {} };
}
connect();
</script></body></html>
"""

async def _ui_index(request):
    return web.Response(text=INDEX_HTML, content_type="text/html")

def _normalize_query(raw: str) -> str:
    """Strip StockX URL prefix and convert spaces → dashes for slug matching."""
    q = raw.lower().strip()
    # Accept full URLs: https://stockx.com/some-slug or stockx.com/some-slug
    for prefix in ("https://stockx.com/", "http://stockx.com/", "stockx.com/"):
        if q.startswith(prefix):
            q = q[len(prefix):]
            break
    # Remove trailing query-string / fragment
    q = q.split("?")[0].split("#")[0].rstrip("/")
    # Spaces → dashes so "adidas vl court" matches "adidas-vl-court-…"
    q = q.replace(" ", "-")
    return q

async def _ui_search(request):
    qs = _normalize_query(request.query.get("q", ""))
    if len(qs) < 2:
        return web.json_response([])
    limit = min(int(request.query.get("limit", 200)), 500)
    out = []
    for slug in _master_slugs:
        if qs in slug:
            out.append(slug)
            if len(out) >= limit:
                break
    return web.json_response(out)

async def _ui_count(request):
    return web.json_response({"total": len(_master_slugs)})

async def _ui_list(request):
    offset = max(0, int(request.query.get("offset", 0)))
    limit  = min(int(request.query.get("limit", 200)), 1000)
    page = _master_slugs[offset:offset+limit]
    return web.json_response([_summary(s) for s in page])

async def _ui_summaries(request):
    body = await request.json()
    slugs = body.get("slugs", [])[:500]
    return web.json_response([_summary(s) for s in slugs])

async def _ui_item(request):
    slug = request.match_info["slug"]
    entry = _cache_ref["data"].get(slug)
    if not entry:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({
        "slug":     slug,
        "id":       entry.get("id"),
        "title":    entry.get("title"),
        "prices":   entry.get("prices", {}),
        "imageUrl": _image_map.get(slug),
    })

async def _ui_history(request):
    slug = request.match_info["slug"]
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, db_query_history, slug)
    return web.json_response(data)

async def _ui_stream(request):
    resp = web.StreamResponse(headers={
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    await resp.prepare(request)
    q: asyncio.Queue = asyncio.Queue(maxsize=2000)
    _subscribers.append(q)
    try:
        await resp.write(b": connected\n\n")
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=20)
                await resp.write(f"data: {json.dumps(item)}\n\n".encode())
            except asyncio.TimeoutError:
                await resp.write(b": ka\n\n")
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    except Exception:
        pass
    finally:
        try: _subscribers.remove(q)
        except ValueError: pass
    return resp

async def start_ui(port: int):
    app = web.Application()
    app.router.add_get("/", _ui_index)
    app.router.add_get("/api/search", _ui_search)
    app.router.add_get("/api/count", _ui_count)
    app.router.add_get("/api/list", _ui_list)
    app.router.add_post("/api/summaries", _ui_summaries)
    app.router.add_get("/api/item/{slug}", _ui_item)
    app.router.add_get("/api/history/{slug}", _ui_history)
    app.router.add_get("/api/stream", _ui_stream)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    print(f"[UI] http://127.0.0.1:{port}/")
    return runner

# ── MAIN ──────────────────────────────────────────────────────────────────────

async def main():
    slugs = [l.strip() for l in Path(SLUGS_FILE).read_text().splitlines() if l.strip()]
    print(f"[OK] {len(slugs)} slugs loaded from {SLUGS_FILE}")
    print(f"[OK] webhook={'configured' if WEBHOOK_URL and not WEBHOOK_URL.startswith('https://YOUR_') else 'DISABLED (log-only)'}")

    cache = load_cache()
    _cache_ref["data"] = cache
    _master_slugs[:] = sorted(slugs)
    sx = StockXFetcher()
    await sx.start()

    ui_runner = await start_ui(UI_PORT)

    global _image_map, _db_conn
    _image_map = load_image_map()
    _db_conn = init_db()

    # Build initial deque sorted oldest-checked first (never-checked = 0 → front)
    # After processing a chunk it is appended to the back, forming a continuous
    # rotating queue: the items that were just checked are always last in line.
    work_queue: deque[str] = deque(
        sorted(slugs, key=lambda s: cache.get(s, {}).get("last_checked", 0))
    )
    total_slugs = len(work_queue)
    print(f"[SCHED] deque built: {total_slugs} slugs, oldest-checked first")

    # Track progress across one full rotation so we can log a summary
    round_checked = 0      # slugs processed in the current rotation
    round_ok = 0
    round_new = 0
    round_items = 0
    round_sizes = 0
    round_start = time.time()
    chunk_num = 0          # global counter (for mem-reset scheduling)

    try:
        async with aiohttp.ClientSession() as http:
            while True:
                await sx.maybe_refresh()

                # ── pull next chunk from the front of the queue ─────────────
                if not work_queue:
                    # Shouldn't happen, but guard: rebuild from cache
                    print("[WARN] work_queue empty — rebuilding from cache")
                    work_queue.extend(
                        sorted(slugs, key=lambda s: cache.get(s, {}).get("last_checked", 0))
                    )

                chunk = []
                for _ in range(CHUNK_SIZE):
                    if work_queue:
                        chunk.append(work_queue.popleft())

                chunk_num += 1
                chunk_t0 = time.time()
                progress = f"{total_slugs - len(work_queue)}/{total_slugs}"
                print(f"  [chunk #{chunk_num}] Starting ({len(chunk)} slugs) | queue pos {progress}")

                # ── periodic tab memory management ───────────────────────
                if chunk_num % 20 == 0:
                    await sx.hard_reset()
                elif chunk_num % 5 == 0:
                    await sx.light_reset()

                # Yield the event loop between chunks
                await asyncio.sleep(0.25)

                try:
                    # Retry the chunk up to 2 extra times on full failure
                    for chunk_attempt in range(3):
                        results = await sx.fetch_batch(chunk)
                        ok_count_check = sum(1 for r in results if r is not None)
                        if ok_count_check > 0 or chunk_attempt == 2:
                            break
                        print(f"  [chunk #{chunk_num}] all None (attempt {chunk_attempt+1}/3), retrying in 3s…")
                        await asyncio.sleep(3)

                    chunk_fetch_elapsed = time.time() - chunk_t0
                    none_count = sum(1 for r in results if r is None)
                    print(f"  [chunk #{chunk_num}] fetch done in {chunk_fetch_elapsed:.1f}s — ok={ok_count_check} none={none_count}/{len(chunk)}")

                    now = time.time()
                    records, new_count, ok_count, size_changed_count = [], 0, 0, 0
                    db_rows: list[tuple] = []
                    ts_int = int(now)
                    for slug, data in zip(chunk, results):
                        if not data:
                            continue
                        ok_count += 1
                        pid, title, variants, new_prices = parse_product(data)
                        if not variants:
                            continue
                        slug_cache = cache.get(slug, {})
                        old_prices = slug_cache.get("prices", {})
                        if not slug_cache.get("variants"):
                            # First time seen — snapshot all sizes into history DB
                            new_count += 1
                            for size, p in new_prices.items():
                                db_rows.append((slug, size, ts_int,
                                                p.get("lowestAsk"), p.get("highestBid"), p.get("lastSale")))
                        else:
                            size_entries = []
                            changed_here = 0
                            for size, p in new_prices.items():
                                old = old_prices.get(size, {})
                                changed = (old.get("lowestAsk")  != p["lowestAsk"]) \
                                       or (old.get("highestBid") != p["highestBid"])
                                if changed:
                                    changed_here += 1
                                    print(f"[$$] {slug} | {size} | ask {old.get('lowestAsk')} -> {p['lowestAsk']} | bid {old.get('highestBid')} -> {p['highestBid']}")
                                    _notify({
                                        "slug": slug, "size": size,
                                        "old": {"ask": old.get("lowestAsk"), "bid": old.get("highestBid")},
                                        "new": {"ask": p["lowestAsk"],       "bid": p["highestBid"]},
                                    })
                                    db_rows.append((slug, size, ts_int,
                                                    p.get("lowestAsk"), p.get("highestBid"), p.get("lastSale")))

                        cache[slug] = {
                            "last_checked": ts_int,
                            "pid": pid,
                            "title": title,
                            "variants": variants,
                            "prices": new_prices,
                        }

                    db_write(db_rows)
                    if chunk_num % 10 == 0:
                        save_cache(cache)

                except Exception as e:
                    print(f"  [chunk #{chunk_num}] error: {type(e).__name__}: {e}")
                    chunk_fetch_elapsed = time.time() - chunk_t0

                wait = max(0, POLL_INTERVAL - chunk_fetch_elapsed)
                if wait:
                    await asyncio.sleep(wait)
    finally:
        save_cache(cache)
        await sx.stop()
        try: await ui_runner.cleanup()
        except Exception: pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[OK] Stopped.")
