import websocket
import json
import time
import os
import logging
import redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BinanceWS')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_STREAM = os.environ.get('REDIS_STREAM', 'binance:depth')
BINANCE_SYMBOL_LIMIT = int(os.environ.get('BINANCE_SYMBOL_LIMIT', '1'))
BINANCE_SYMBOLS = os.environ.get('BINANCE_SYMBOLS', 'btcusdt').strip()
BINANCE_STREAM_TYPE = os.environ.get('BINANCE_STREAM_TYPE', 'depth20')

logger.info(f"Kết nối tới Redis tại {REDIS_HOST}:{REDIS_PORT}, stream: {REDIS_STREAM}")


def _create_redis_client():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
        retry_on_timeout=True, socket_connect_timeout=5, socket_timeout=5,
    )


r = _create_redis_client()

message_count = 0
depth_update_count = 0
total_input_bytes = 0
total_output_bytes = 0
symbols = []
start_time = time.time()
last_stats_time = start_time
STATS_EVERY_MESSAGES = int(os.environ.get('BINANCE_STATS_EVERY_MESSAGES', '100'))
STATS_EVERY_SECONDS = float(os.environ.get('BINANCE_STATS_EVERY_SECONDS', '10'))


def _format_bytes(num_bytes):
    value = float(num_bytes)
    for unit in ('B', 'KB', 'MB', 'GB'):
        if value < 1024 or unit == 'GB':
            return f"{value:.2f} {unit}"
        value /= 1024


def _message_size_bytes(message):
    if isinstance(message, bytes):
        return len(message)
    return len(message.encode('utf-8'))


def _log_input_stats(force=False):
    global last_stats_time
    now = time.time()
    elapsed_since_log = now - last_stats_time
    should_log_by_count = STATS_EVERY_MESSAGES > 0 and message_count % STATS_EVERY_MESSAGES == 0
    should_log_by_time = STATS_EVERY_SECONDS > 0 and elapsed_since_log >= STATS_EVERY_SECONDS

    if not force and not should_log_by_count and not should_log_by_time:
        return

    elapsed = max(now - start_time, 1e-9)
    avg_message_size = total_input_bytes / message_count if message_count else 0
    message_rate = message_count / elapsed
    byte_rate = total_input_bytes / elapsed

    gb_factor = 1024.0 ** 3
    input_gb = total_input_bytes / gb_factor
    output_gb = total_output_bytes / gb_factor

    logger.info(
        "[Binance input] "
        f"messages={message_count}, "
        f"depth_updates={depth_update_count}, "
        f"input={_format_bytes(total_input_bytes)} ({input_gb:.8f} GB), "
        f"output={_format_bytes(total_output_bytes)} ({output_gb:.8f} GB), "
        f"avg_msg={_format_bytes(avg_message_size)}, "
        f"rate={message_rate:.2f} msg/s, "
        f"bandwidth={_format_bytes(byte_rate)}/s"
    )
    last_stats_time = now

try:
    r.delete(REDIS_STREAM)
    logger.info(f"Đã làm sạch stream cũ '{REDIS_STREAM}' trên Redis.")
except (redis.ConnectionError, redis.TimeoutError) as e:
    logger.warning(f"Không thể làm sạch stream cũ (Redis unavailable): {e}")
    r = None

def validate_side(side_list):
    """Validate one order-book side and keep raw Binance values unchanged."""
    valid = []
    for entry in side_list:
        if len(entry) >= 2:
            valid.append([entry[0], entry[1]])
    return valid


def _get_redis():
    """Return a working Redis client, reconnecting if needed"""
    global r
    if r is None:
        r = _create_redis_client()
    try:
        r.ping()
        return r
    except (redis.ConnectionError, redis.TimeoutError):
        logger.warning("Redis connection lost, reconnecting...")
        try:
            r = _create_redis_client()
            r.ping()
            return r
        except (redis.ConnectionError, redis.TimeoutError):
            r = None
            return None


def on_message(ws, message):
    global r, message_count, depth_update_count, total_input_bytes, total_output_bytes
    try:
        message_count += 1
        total_input_bytes += _message_size_bytes(message)

        payload = json.loads(message)
        if "data" in payload and "stream" in payload:
            data = payload["data"]
        else:
            data = payload

        if 'bids' in data and 'asks' in data:
            data = {
                'e': 'depthSnapshot',
                'E': int(time.time() * 1000),
                'b': data.get('bids', []),
                'a': data.get('asks', []),
                'is_snapshot': True,
            }
        elif data.get('e') == 'depthUpdate':
            data['is_snapshot'] = False
        else:
            _log_input_stats()
            return

        depth_update_count += 1

        if 'b' in data:
            data['b'] = validate_side(data['b'])

        if 'a' in data:
            data['a'] = validate_side(data['a'])

        client = _get_redis()
        if client is not None:
            redis_data = json.dumps(data)
            client.xadd(REDIS_STREAM, {'data': redis_data})
            total_output_bytes += len(redis_data.encode('utf-8'))
        else:
            logger.warning("Redis unavailable — dropping depth update")

        _log_input_stats()

    except json.JSONDecodeError as e:
        logger.error(f"Lỗi parse JSON: {e}")
    except redis.RedisError as e:
        logger.error(f"Lỗi Redis: {e}")
        r = None
    except Exception as e:
        logger.error(f"Lỗi xử lý message: {e}", exc_info=True)


def on_error(ws, error):
    logger.error(f"Lỗi WebSocket: {error}")


def on_close(ws, close_status_code, close_msg):
    _log_input_stats(force=True)
    logger.info(f"Đóng kết nối WebSocket. Status: {close_status_code}, Msg: {close_msg}")


def on_open(ws):
    global symbols
    active_syms = symbols if symbols else ["btcusdt"]
    stream_names = [f"{sym}@{BINANCE_STREAM_TYPE}@100ms" for sym in active_syms]
    
    logger.info(f"WebSocket connection opened. Subscribing to {len(active_syms)} streams...")
    for i in range(0, len(stream_names), 100):
        batch = stream_names[i:i+100]
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": batch,
            "id": i + 1
        }
        ws.send(json.dumps(subscribe_message))
        logger.info(f"Sent subscription batch #{i//100 + 1} with {len(batch)} streams.")


MAX_RECONNECT_DELAY = 60


def run_with_reconnect(url, max_retries=None):
    """Run WebSocket with automatic reconnect logic"""
    retries = 0
    while max_retries is None or retries < max_retries:
        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        retries += 1
        delay = min(5 * retries, MAX_RECONNECT_DELAY)
        logger.warning(f"WebSocket disconnected. Reconnecting in {delay}s (retry #{retries})...")
        time.sleep(delay)


if __name__ == "__main__":
    configured_symbols = BINANCE_SYMBOLS

    if configured_symbols and configured_symbols.upper() != "AUTO":
        symbols = [
            symbol.strip().lower()
            for symbol in configured_symbols.split(',')
            if symbol.strip()
        ]
        logger.info(f"Using {len(symbols)} symbols from BINANCE_SYMBOLS.")
    else:
        try:
            import requests
            logger.info("Fetching active symbols from Binance Exchange Info API...")
            resp = requests.get('https://api.binance.com/api/v3/exchangeInfo', timeout=10).json()
            active_symbols = []
            for s in resp.get('symbols', []):
                if s.get('status') == 'TRADING':
                    sym = s.get('symbol', '').lower()
                    if any(sym.endswith(quote) for quote in ['usdt', 'usdc', 'fdusd', 'btc', 'eth', 'bnb']):
                        active_symbols.append(sym)
            symbols = sorted(list(set(active_symbols)))[:BINANCE_SYMBOL_LIMIT]
            logger.info(f"Successfully retrieved {len(symbols)} active symbols from API.")
        except Exception as e:
            logger.warning(f"Failed to fetch symbols dynamically: {e}. Falling back to default list.")
            symbols = [
                "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt", "adausdt", "dogeusdt", "trxusdt", "linkusdt",
                "maticusdt", "dotusdt", "ltcusdt", "uniusdt", "aptusdt", "nearusdt", "filusdt", "opusdt", "arbusdt",
                "avaxusdt", "shibusdt", "atomusdt", "etcusdt", "ldousdt", "imxusdt", "grtusdt", "rndrusdt", "injusdt",
                "pepeusdt", "suiusdt", "tiausdt", "seiusdt", "wifusdt", "ftmusdt", "thetausdt", "vetusdt", "algousdt",
                "egldusdt", "flowusdt", "axsusdt", "sandusdt", "manausdt", "chzusdt", "galausdt", "dydxusdt", "crvusdt",
                "mkrusdt", "aaveusdt", "compusdt", "snxusdt", "runeusdt"
            ][:BINANCE_SYMBOL_LIMIT]

    default_url = "wss://stream.binance.com:9443/ws"
    url = os.environ.get('BINANCE_WS_URL', default_url)
    logger.info(f"Connecting to {url} to subscribe to {len(symbols)} streams...")
    run_with_reconnect(url)
