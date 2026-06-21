"""
Redis Manager — Centralized Redis integration for the Market Manipulation Detection System.

Provides:
  - Alert publishing via Redis Pub/Sub (real-time notifications)
  - Detection result storage in Redis Sorted Sets (history with timestamps)
  - LOB snapshot caching in Redis Hashes (for analysis replay)
  - Stream health monitoring
  - Connection pooling and resilience
"""
import os
import json
import time
import logging
import threading

import redis

logger = logging.getLogger("RedisManager")

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', '')
REDIS_DB = int(os.environ.get('REDIS_DB', '0'))

ALERT_CHANNEL = "market:alerts"
DETECTION_KEY = "market:detections"
LOB_SNAPSHOT_KEY = "market:lob:snapshot"
LOB_HISTORY_KEY = "market:lob:history"
HEALTH_KEY = "market:health"


class RedisManager:
    """Centralized Redis client with connection pooling and all detection-system keys."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, host=None, port=None, password=None, db=None):
        if self._initialized:
            requested_host = host or REDIS_HOST
            requested_port = port or REDIS_PORT
            requested_password = password or REDIS_PASSWORD
            requested_db = db or REDIS_DB
            if (requested_host != self._host or
                requested_port != self._port or
                requested_password != self._password or
                requested_db != self._db):
                logger.warning(
                    f"RedisManager already initialized with {self._host}:{self._port} (db={self._db}). "
                    f"Ignoring new initialization parameters: {requested_host}:{requested_port} (db={requested_db})."
                )
            return
        self._host = host or REDIS_HOST
        self._port = port or REDIS_PORT
        self._password = password or REDIS_PASSWORD
        self._db = db or REDIS_DB
        self._pool = redis.ConnectionPool(
            host=self._host,
            port=self._port,
            password=self._password if self._password else None,
            db=self._db,
            max_connections=10,
            socket_timeout=5,
            socket_connect_timeout=3,
            retry_on_timeout=True
        )
        self._client = redis.Redis(connection_pool=self._pool, decode_responses=True)
        self._connected = None
        self._last_check_time = 0.0
        self._initialized = True
        logger.info(f"RedisManager initialized: {self._host}:{self._port}")

    def _check_connection(self):
        current_time = time.time()
        if self._connected is False and (current_time - self._last_check_time < 10.0):
            return False
            
        try:
            self._client.ping()
            self._connected = True
            self._last_check_time = current_time
            return True
        except redis.RedisError as e:
            if self._connected is not False:
                logger.warning(f"Redis connection check failed: {e}. Running in offline mode.")
            self._connected = False
            self._last_check_time = current_time
            return False

    def publish_alert(self, alert_data: dict):
        """
        Publish a manipulation detection alert to the Pub/Sub channel.
        Subscribers (dashboards, notification services) receive alerts instantly.
        """
        if not self._check_connection():
            logger.warning("Redis unavailable — alert not published.")
            return False

        alert_data["published_at"] = time.time()
        message = json.dumps(alert_data)
        try:
            receivers = self._client.publish(ALERT_CHANNEL, message)
            logger.info(f"Alert published to {receivers} subscriber(s): {alert_data.get('type', 'unknown')}")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to publish alert: {e}")
            return False


    def store_detection_result(self, result_data: dict):
        """
        Store a detection result in a Redis Sorted Set, scored by timestamp.
        Enables time-range queries and replay of past detections.
        """
        if not self._check_connection():
            logger.warning("Redis unavailable — detection result not stored.")
            return False

        timestamp = result_data.get("timestamp", time.time())
        result_data["stored_at"] = timestamp
        member = json.dumps(result_data)
        try:
            self._client.zadd(DETECTION_KEY, {member: timestamp})
            self._client.zremrangebyrank(DETECTION_KEY, 0, -1001)
            logger.info(f"Detection result stored (timestamp={timestamp:.2f})")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to store detection result: {e}")
            return False

    def get_detection_history(self, since: float = 0, limit: int = 100):
        """
        Retrieve detection results from a given timestamp onwards.
        Returns list of dicts sorted by time ascending.
        """
        if not self._check_connection():
            return []

        try:
            raw = self._client.zrangebyscore(DETECTION_KEY, since, "+inf", start=0, num=limit)
            return [json.loads(item) for item in raw]
        except redis.RedisError as e:
            logger.error(f"Failed to get detection history: {e}")
            return []

    def cache_lob_snapshot(self, lob_state):
        """
        Cache the current C++ LOB state in Redis as a JSON Hash.
        Enables fast retrieval for analysis replay and cross-process sharing.
        """
        if not self._check_connection():
            logger.warning("Redis unavailable — LOB snapshot not cached.")
            return False

        orders = lob_state.get_all_orders()
        spread = lob_state.get_spread()
        if spread == float('inf') or spread != spread:
            spread = 999999999.0

        snapshot = {
            "timestamp": time.time(),
            "best_bid": lob_state.get_best_bid(),
            "best_ask": lob_state.get_best_ask(),
            "spread": spread,
            "cancellation_rate": lob_state.get_cancellation_rate(),
            "num_orders": len(orders),
            "suspect_order_id": lob_state.get_suspect_order_id(),
            "market_order_volume": lob_state.get_market_order_volume()
        }

        orders_json = json.dumps([
            {
                "id": o.id, "price": o.price, "volume": o.volume,
                "side": o.side.name,
                "timestamp": o.timestamp
            } for o in orders
        ])

        try:
            self._client.hset(LOB_SNAPSHOT_KEY, mapping=snapshot)
            self._client.set(f"{LOB_SNAPSHOT_KEY}:orders", orders_json)
            self._client.zadd(LOB_HISTORY_KEY, {json.dumps(snapshot): snapshot["timestamp"]})
            self._client.zremrangebyrank(LOB_HISTORY_KEY, 0, -101)
            logger.info(f"LOB snapshot cached: {len(orders)} orders, spread={snapshot['spread']}")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to cache LOB snapshot: {e}")
            return False

    def get_lob_snapshot(self):
        """Retrieve the latest cached LOB snapshot metadata."""
        if not self._check_connection():
            return None

        try:
            data = self._client.hgetall(LOB_SNAPSHOT_KEY)
            if data:
                orders_raw = self._client.get(f"{LOB_SNAPSHOT_KEY}:orders")
                if orders_raw:
                    data["orders"] = json.loads(orders_raw)
                return data
            return None
        except redis.RedisError as e:
            logger.error(f"Failed to get LOB snapshot: {e}")
            return None

    def report_health(self, component: str, status: str, details: dict = None):
        """
        Store component health status in Redis for monitoring dashboards.
        """
        if not self._check_connection():
            return False

        health_data = {
            "component": component,
            "status": status,
            "timestamp": time.time(),
            "details": json.dumps(details or {})
        }
        try:
            self._client.hset(HEALTH_KEY, component, json.dumps(health_data))
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to report health: {e}")
            return False

    def get_health(self):
        """Get all component health statuses."""
        if not self._check_connection():
            return {}

        try:
            raw = self._client.hgetall(HEALTH_KEY)
            return {k: json.loads(v) for k, v in raw.items()}
        except redis.RedisError as e:
            logger.error(f"Failed to get health: {e}")
            return {}

    def get_stream_length(self, stream_name: str):
        """Get the number of entries in a Redis Stream."""
        if not self._check_connection():
            return 0

        try:
            info = self._client.xinfo_stream(stream_name)
            return info.get("length", 0)
        except redis.RedisError:
            return 0

    def cleanup_stream(self, stream_name: str, max_length: int = 10000):
        """Trim a Redis Stream to keep only the most recent entries."""
        if not self._check_connection():
            return False

        try:
            self._client.xtrim(stream_name, maxlen=max_length, approximate=True)
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to trim stream: {e}")
            return False

def subscribe_alerts(host=None, port=None, password=None):
    """
    Standalone subscriber function for alert channel.
    Can be run in a separate process/thread to listen for alerts.
    """
    effective_password = password or REDIS_PASSWORD or None
    client = redis.Redis(
        host=host or REDIS_HOST,
        port=port or REDIS_PORT,
        password=effective_password if effective_password else None,
        decode_responses=True
    )
    pubsub = client.pubsub()
    pubsub.subscribe(ALERT_CHANNEL)
    logger.info(f"Subscribed to alert channel: {ALERT_CHANNEL}")

    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            severity = data.get("severity", "UNKNOWN")
            alert_type = data.get("type", "unknown")
            print(f"[{severity}] {alert_type}: avg_risk={data.get('avg_risk')}, max_risk={data.get('max_risk')}")