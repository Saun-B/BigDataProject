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

logger.info(f"Kết nối tới Redis tại {REDIS_HOST}:{REDIS_PORT}, stream: {REDIS_STREAM}")


def _create_redis_client():
    return redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
        retry_on_timeout=True, socket_connect_timeout=5, socket_timeout=5,
    )


r = _create_redis_client()

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
    global r
    try:
        data = json.loads(message)
        if data.get('e') != 'depthUpdate':
            return

        if 'b' in data:
            data['b'] = validate_side(data['b'])

        if 'a' in data:
            data['a'] = validate_side(data['a'])

        client = _get_redis()
        if client is not None:
            client.xadd(REDIS_STREAM, {'data': json.dumps(data)})
        else:
            logger.warning("Redis unavailable — dropping depth update")

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
    logger.info(f"Đóng kết nối WebSocket. Status: {close_status_code}, Msg: {close_msg}")


def on_open(ws):
    logger.info("Mở kết nối tới Binance WebSocket: BTCUSDT@depth")


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
    url = os.environ.get('BINANCE_WS_URL', "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms")
    logger.info(f"Kết nối tới: {url}")
    run_with_reconnect(url)
