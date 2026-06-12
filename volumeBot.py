import asyncio
import time
from statistics import median

import aiohttp

# ================= CONFIG =================
TELEGRAM_TOKEN = "PUT_NEW_TOKEN_HERE"
TELEGRAM_CHAT_ID = "PUT_CHAT_ID_HERE"

TIMEFRAME = "5m"
LOOKBACK_PERIOD = 20

MULTIPLIER = 10
MIN_AV_VOLUME_USDT = 10_000

MIN_PRICE_MOVE = 2.0          # %
ALERT_COOLDOWN = 3600         # 1 hour
MAX_ALERT_CACHE = 10000

BINANCE_BASE_URL = "https://api.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"

CONCURRENCY = 40
TELEGRAM_RATE_LIMIT_SECONDS = 1.0

# ================= STATE =================
last_alerts = {}
last_telegram_send = 0.0


# ================= HELPERS =================
def timeframe_to_bybit_interval(tf: str) -> str:
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
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text
    }

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


# ================= SYMBOLS =================
async def get_binance_symbols(session):
    url = f"{BINANCE_BASE_URL}/api/v3/exchangeInfo"
    data = await fetch_json(session, url)

    if not data:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    return [
        s["symbol"]
        for s in data.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
    ]


async def get_bybit_symbols(session):
    url = f"{BYBIT_BASE_URL}/v5/market/instruments-info"
    params = {"category": "spot"}
    data = await fetch_json(session, url, params=params)

    if not data:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

    try:
        res = data.get("result", {}).get("list", [])
        return [
            m["symbol"]
            for m in res
            if str(m.get("symbol", "")).endswith("USDT")
        ]
    except Exception:
        return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


# ================= SCAN BINANCE =================
async def scan_binance_symbol(session, symbol):
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": LOOKBACK_PERIOD + 1}

    data = await fetch_json(session, url, params=params)
    if not data or len(data) < LOOKBACK_PERIOD + 1:
        return

    try:
        current = data[-1]
        prev = data[:-1]

        current_price = float(current[4])
        current_volume = float(current[5])
        current_open = float(current[1])
        current_ts = int(current[0])

        avg_volume = median(float(k[5]) for k in prev)

        await evaluate_and_alert(
            session,
            "Binance",
            symbol,
            avg_volume,
            current_volume,
            current_price,
            current_open,
            current_ts
        )

    except Exception:
        return


# ================= SCAN BYBIT =================
async def scan_bybit_symbol(session, symbol):
    url = f"{BYBIT_BASE_URL}/v5/market/kline"
    interval = timeframe_to_bybit_interval(TIMEFRAME)

    params = {
        "category": "spot",
        "symbol": symbol,
        "interval": interval,
        "limit": LOOKBACK_PERIOD + 1
    }

    data = await fetch_json(session, url, params=params)
    if not data:
        return

    try:
        klines = data.get("result", {}).get("list", [])
        if len(klines) < LOOKBACK_PERIOD + 1:
            return

        klines = sorted(klines, key=lambda x: int(x[0]))

        current = klines[-1]
        prev = klines[:-1]

        current_price = float(current[4])
        current_volume = float(current[5])
        current_open = float(current[1])
        current_ts = int(current[0])

        avg_volume = median(float(k[5]) for k in prev)

        await evaluate_and_alert(
            session,
            "Bybit",
            symbol,
            avg_volume,
            current_volume,
            current_price,
            current_open,
            current_ts
        )

    except Exception:
        return


# ================= CORE LOGIC =================
async def evaluate_and_alert(
    session,
    exchange,
    symbol,
    avg_volume,
    current_volume,
    current_price,
    current_open,
    current_ts
):
    if avg_volume <= 0:
        return

    avg_volume_usdt = avg_volume * current_price
    current_volume_usdt = current_volume * current_price

    if avg_volume_usdt < MIN_AV_VOLUME_USDT:
        return

    # volume spike condition
    if current_volume < avg_volume * MULTIPLIER:
        return

    # price movement filter
    price_move = abs((current_price - current_open) / current_open) * 100
    if price_move < MIN_PRICE_MOVE:
        return

    ratio = current_volume / avg_volume
    increase_pct = (ratio - 1) * 100

    key = f"{exchange}:{symbol}"
    now = time.time()

    # cooldown
    if key in last_alerts:
        if now - last_alerts[key] < ALERT_COOLDOWN:
            return

    msg = (
        f"🚀 VOLUME SPIKE 🚀\n\n"
        f"Exchange: {exchange}\n"
        f"Symbol: {symbol}\n"
        f"Price: ${current_price:,.6f}\n"
        f"Price Move: {price_move:.2f}%\n"
        f"Spike: {ratio:.1f}x ({increase_pct:.0f}%)\n"
        f"Current Vol: ${current_volume_usdt:,.0f}\n"
        f"Avg Vol: ${avg_volume_usdt:,.0f}"
    )

    await send_telegram(session, msg)

    last_alerts[key] = now

    # cleanup memory
    if len(last_alerts) > MAX_ALERT_CACHE:
        oldest = sorted(last_alerts.items(), key=lambda x: x[1])[:1000]
        for k, _ in oldest:
            last_alerts.pop(k, None)


# ================= MAIN LOOP =================
async def main():
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        binance_symbols = await get_binance_symbols(session)
        bybit_symbols = await get_bybit_symbols(session)

        print(f"Loaded Binance: {len(binance_symbols)} | Bybit: {len(bybit_symbols)}")

        await send_telegram(session, "📡 Volume scanner started")

        while True:
            start = time.time()
            tasks = []

            for s in binance_symbols:
                await sem.acquire()
                task = asyncio.create_task(scan_binance_symbol(session, s))
                task.add_done_callback(lambda t: sem.release())
                tasks.append(task)

            for s in bybit_symbols:
                await sem.acquire()
                task = asyncio.create_task(scan_bybit_symbol(session, s))
                task.add_done_callback(lambda t: sem.release())
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start
            await asyncio.sleep(max(0, 25 - elapsed))


if _name_ == "_main_":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
