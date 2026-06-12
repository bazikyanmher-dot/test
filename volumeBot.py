import asyncio
import time
import math
from collections import defaultdict, deque

import aiohttp

# --- CONFIG ---
TELEGRAM_TOKEN = "8699424396:AAFMRzEUicnEpVsPJwf3gewnXKBemZy7gJY"
TELEGRAM_CHAT_ID = "568017430"

TIMEFRAME = "5m"            # '1m','3m','5m','15m',...
LOOKBACK_PERIOD = 20        # number of previous candles to average
MULTIPLIER = 10             # spike threshold (current >= avg * MULTIPLIER)
MIN_AV_VOLUME_USDT = 10_000 # require avg volume (USDT) to exceed this to ignore low-liquidity coins

BINANCE_BASE_URL = "https://api.binance.com"   # change to binance.us if needed
BYBIT_BASE_URL = "https://api.bybit.com"       # change if you use different domain

CONCURRENCY = 40
TELEGRAM_RATE_LIMIT_SECONDS = 1.0  # min seconds between telegram sends

# --- state ---
already_alerted = {}  # key -> candle_timestamp
last_telegram_send = 0.0

# --- helpers ---
def timeframe_to_bybit_interval(tf: str) -> str:
    # supports common TFs; Bybit expects minutes as numbers for spot (e.g., '5' for 5m)
    if tf.endswith("m"):
        return str(int(tf[:-1]))
    mapping = {"1h": "60", "4h": "240", "1d": "D"}
    return mapping.get(tf, tf)

async def send_telegram(session, text):
    global last_telegram_send
    now = time.time()
    if now - last_telegram_send < TELEGRAM_RATE_LIMIT_SECONDS:
        await asyncio.sleep(TELEGRAM_RATE_LIMIT_SECONDS - (now - last_telegram_send))
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        async with session.post(url, json=payload, timeout=15) as r:
            await r.text()
    except Exception:
        pass
    last_telegram_send = time.time()

async def fetch_json(session, url, params=None, retries=2, timeout=10):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, params=params, timeout=timeout) as r:
                return await r.json()
        except Exception:
            if attempt == retries:
                return None
            await asyncio.sleep(0.5 + attempt)

# --- symbol loaders ---
async def get_binance_symbols(session):
    url = f"{BINANCE_BASE_URL}/api/v3/exchangeInfo"
    data = await fetch_json(session, url)
    if not data or "symbols" not in data:
        # fallback sample
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    symbols = []
    for s in data["symbols"]:
        if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT":
            symbols.append(s["symbol"])
    return symbols

async def get_bybit_symbols(session):
    # Bybit spot instruments info
    url = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
    params = {"category": "spot"}
    data = await fetch_json(session, url, params=params)
    if not data:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    try:
        res = data.get("result", {}).get("list", [])
        return [m["symbol"] for m in res if str(m.get("symbol","")).endswith("USDT")]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# --- scanning logic ---
async def scan_binance_symbol(session, symbol):
    # klines: [ openTime, open, high, low, close, volume, ... ]
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": LOOKBACK_PERIOD + 1}
    data = await fetch_json(session, url, params=params)
    if not data or len(data) < LOOKBACK_PERIOD + 1:
        return
    try:
        # each k: [ts, o, h, l, c, v, ...]
        prev = data[:-1]
        cur = data[-1]
        avg_volume = sum(float(k[5]) for k in prev) / len(prev)
        current_volume = float(cur[5])
        current_price = float(cur[4])
        current_ts = int(cur[0])
        await evaluate_and_alert(session, "Binance", symbol, avg_volume, current_volume, current_price, current_ts)
    except Exception:
        return

async def scan_bybit_symbol(session, symbol):
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    interval = timeframe_to_bybit_interval(TIMEFRAME)
    params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": LOOKBACK_PERIOD + 1}
    data = await fetch_json(session, url, params=params)
    if not data:
        return
    try:
        # Bybit returns result.list as [ [t,o,h,l,c,v,turnover], ... ] with newest last typically
        result = data.get("result", {})
        klines = result.get("list") or []
        if len(klines) < LOOKBACK_PERIOD + 1:
            return
        prev = klines[:-1]
        cur = klines[-1]
        avg_volume = sum(float(k[5]) for k in prev) / len(prev)
        current_volume = float(cur[5])
        current_price = float(cur[4])
        current_ts = int(cur[0])
        await evaluate_and_alert(session, "Bybit", symbol, avg_volume, current_volume, current_price, current_ts)
    except Exception:
        return

async def evaluate_and_alert(session, exchange, symbol, avg_volume, current_volume, current_price, current_ts):
    if avg_volume <= 0:
        return
    avg_volume_usdt = avg_volume * current_price
    current_volume_usdt = current_volume * current_price
    if avg_volume_usdt < MIN_AV_VOLUME_USDT:
        return
    if current_volume >= avg_volume * MULTIPLIER:
        key = f"{exchange}:{symbol}"
        if already_alerted.get(key) == current_ts:
            return
        ratio = current_volume / avg_volume
        increase_pct = (ratio - 1) * 100
        msg = (
            f"🚀 *VOLUME SPIKE* 🚀\n"
            f"*Exchange:* `{exchange}`\n"
            f"*Symbol:* `{symbol}`\n"
            f"*Price:* `${current_price:,.6f}`\n"
            f"*Spike:* {ratio:.1f}x ({increase_pct:.0f}% over avg)\n"
            f"*Candle Vol (USDT):* `${current_volume_usdt:,.0f}`\n"
            f"*Avg Vol (USDT):* `${avg_volume_usdt:,.0f}`\n"
        )
        await send_telegram(session, msg)
        already_alerted[key] = current_ts

# --- main loop ---
async def main():
    sem = asyncio.Semaphore(CONCURRENCY)
    async with aiohttp.ClientSession() as session:
        binance_symbols = await get_binance_symbols(session)
        bybit_symbols = await get_bybit_symbols(session)
        print(f"Loaded {len(binance_symbols)} binance symbols, {len(bybit_symbols)} bybit symbols")
        # optional: limit symbol lists for performance in testing
        # binance_symbols = binance_symbols[:200]
        # bybit_symbols = bybit_symbols[:200]

        # initial notification
        await send_telegram(session, "📡 Volume spike scanner started")

        while True:
            start = time.time()
            tasks = []
            # schedule Binance
            for s in binance_symbols:
                await sem.acquire()
                task = asyncio.create_task(scan_binance_symbol(session, s))
                task.add_done_callback(lambda t: sem.release())
                tasks.append(task)
            # schedule Bybit
            for s in bybit_symbols:
                await sem.acquire()
                task = asyncio.create_task(scan_bybit_symbol(session, s))
                task.add_done_callback(lambda t: sem.release())
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start
            # run roughly every 25 seconds (you can tune)
            wait = max(0, 25 - elapsed)
            await asyncio.sleep(wait)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")
