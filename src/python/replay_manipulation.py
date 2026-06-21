import os
import sys
import json
import time
import logging
import redis
import argparse

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("ReplaySimulation")

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_STREAM = os.environ.get('REDIS_STREAM', 'binance:depth')

def resolve_path(path):
    if os.path.isabs(path):
        return path

    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(os.path.join(src_dir, "../.."))
    return os.path.normpath(os.path.join(project_root, path))

def main():
    parser = argparse.ArgumentParser(description="Replay market events into Redis Stream")
    parser.add_argument(
        "--file",
        default=None,
        help="Replay JSON file. Nếu không truyền thì dùng classic_spoofing_case.json như cũ."
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.1,
        help="Sleep time giữa các event khi chạy file tùy chọn."
    )
    args = parser.parse_args()
    logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")

    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
    except Exception as e:
        logger.error(f"Cannot connect to Redis: {e}. Please ensure Redis is running.")
        sys.exit(1)
        
    src_dir = os.path.dirname(os.path.abspath(__file__))
    if args.file is None:
        json_path = os.path.normpath(os.path.join(src_dir, '../../data/classic_spoofing_case.json'))
        replay_mode = "classic"
    else:
        json_path = resolve_path(args.file)
        replay_mode = "generic"
    
    if not os.path.exists(json_path):
        logger.error(f"Simulation file not found at {json_path}. Please run generate_datasets.py first.")
        sys.exit(1)
        
    logger.info(f"Loading replay data from {json_path}...")
    logger.info(f"Replay mode: {replay_mode}")
    with open(json_path, 'r', encoding='utf-8') as f:
        events = json.load(f)
        
    logger.info(f"Trimming stream key '{REDIS_STREAM}' to length 0...")
    try:
        r.xtrim(REDIS_STREAM, maxlen=0)
    except Exception as e:
        logger.warning(f"Failed to trim stream: {e}. Attempting delete instead.")
        r.delete(REDIS_STREAM)
    
    logger.info(f"Replaying {len(events)} market events into Redis stream...")
    if replay_mode == "generic":
        logger.info("--- Generic Replay Mode: Pushing all events ---")

        for i, event in enumerate(events):
            logger.info(
                f"Event #{i+1}/{len(events)} pushing... "
                f"timestamp={event.get('E')} "
                f"bids={len(event.get('b', []))} "
                f"asks={len(event.get('a', []))}"
            )
            r.xadd(REDIS_STREAM, {'data': json.dumps(event)})
            time.sleep(args.sleep)

        logger.info("Generic replay completed successfully!")
        return
    
    logger.info("--- Pushing Phase 1: Normal (10 events) ---")
    for i in range(0, 10):
        if i < len(events):
            logger.info(f"Event #{i+1}/{len(events)} (Phase 1 - Normal) pushing...")
            r.xadd(REDIS_STREAM, {'data': json.dumps(events[i])})
            time.sleep(0.1)
    logger.info("Sleeping 7 seconds to let Spark process Phase 1...")
    time.sleep(7.0)

    logger.info("--- Pushing Phase 2: Spoof Wall (10 events) ---")
    for i in range(10, 20):
        if i < len(events):
            logger.info(f"Event #{i+1}/{len(events)} (Phase 2 - Spoof Wall) pushing...")
            r.xadd(REDIS_STREAM, {'data': json.dumps(events[i])})
            time.sleep(0.1)
    logger.info("Sleeping 7 seconds to let Spark process Phase 2...")
    time.sleep(7.0)

    logger.info("--- Pushing Phase 3: Price Push (5 events) ---")
    for i in range(20, 25):
        if i < len(events):
            logger.info(f"Event #{i+1}/{len(events)} (Phase 3 - Price Push) pushing...")
            r.xadd(REDIS_STREAM, {'data': json.dumps(events[i])})
            time.sleep(0.1)
    logger.info("Sleeping 7 seconds to let Spark process Phase 3...")
    time.sleep(7.0)

    logger.info("--- Pushing Phase 4: Collapse (5 events) ---")
    for i in range(25, 30):
        if i < len(events):
            logger.info(f"Event #{i+1}/{len(events)} (Phase 4 - Collapse) pushing...")
            r.xadd(REDIS_STREAM, {'data': json.dumps(events[i])})
            time.sleep(0.1)

    logger.info("Simulation replay completed successfully!")

if __name__ == "__main__":
    main()
