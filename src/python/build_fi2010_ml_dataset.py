import argparse
import csv
import json
import os
import sys
import threading
import numpy as np


src_dir = os.path.dirname(os.path.abspath(__file__))

build_dirs = [
    os.path.normpath(os.path.join(src_dir, "../../build")),
    os.path.normpath(os.path.join(src_dir, "../../build-py311-v2")),
    os.path.normpath(os.path.join(src_dir, "../../build-msvc-nmake")),
    os.path.normpath(os.path.join(src_dir, "../../build-msvc/Debug")),
    os.path.normpath(os.path.join(src_dir, "../../build-msvc/Release")),
]

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

for build_dir in build_dirs:
    if build_dir not in sys.path:
        sys.path.insert(0, build_dir)

try:
    import lob_core
except ImportError as e:
    print("[ERROR] Không import được lob_core.")
    print("Hãy kiểm tra đã build C++ LOB chưa.")
    print("Chi tiết:", e)
    sys.exit(1)

from ml_filter import extract_features

VOLUME_SCALE = 100

def resolve_path(path):
    if os.path.isabs(path):
        return path

    project_root = os.path.normpath(os.path.join(src_dir, "../.."))
    return os.path.normpath(os.path.join(project_root, path))


def load_replay_events(path):
    path = resolve_path(path)

    if not os.path.exists(path):
        raise FileNotFoundError(f"Không tìm thấy file replay: {path}")

    with open(path, "r", encoding="utf-8") as f:
        events = json.load(f)

    if not isinstance(events, list):
        raise ValueError("Replay JSON phải là list event.")

    return events

class PriceLevelIdMapper:
    def __init__(self):
        self._id_map = {}
        self._id_counter = 1
        self._lock = threading.Lock()

    def get_id(self, side_prefix, price):
        key = (side_prefix, f"{price:.8f}".rstrip("0").rstrip("."))

        with self._lock:
            if key not in self._id_map:
                self._id_map[key] = self._id_counter
                self._id_counter += 1

            return self._id_map[key]


def replay_to_features(input_json, output_csv):
    events = load_replay_events(input_json)
    output_csv = resolve_path(output_csv)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    lob = lob_core.LOB()
    lob.set_market_order_volume(100)

    id_mapper = PriceLevelIdMapper()

    rows = []

    for event_idx, event in enumerate(events):
        if event_idx % 1000 == 0:
            print(f"[Progress] Processed {event_idx}/{len(events)} events...")
        timestamp = int(event.get("E", event_idx))

        orders_before = set(o.id for o in lob.get_all_orders())

        for bid in event.get("b", []):
            if len(bid) < 2:
                continue

            try:
                price = float(bid[0])
                vol = int(round(float(bid[1]) * VOLUME_SCALE))
            except Exception:
                continue

            order_id = id_mapper.get_id("BID", price)

            lob.cancel_order(order_id)

            if price > 0 and vol > 0:
                lob.add_order(order_id, lob_core.Side.BID, price, vol, timestamp)

        for ask in event.get("a", []):
            if len(ask) < 2:
                continue

            try:
                price = float(ask[0])
                vol = int(round(float(ask[1]) * VOLUME_SCALE))
            except Exception:
                continue

            order_id = id_mapper.get_id("ASK", price)

            lob.cancel_order(order_id)

            if price > 0 and vol > 0:
                lob.add_order(order_id, lob_core.Side.ASK, price, vol, timestamp)

        orders_after = set(o.id for o in lob.get_all_orders())

        genuine_cancels = len(orders_before - orders_after)
        genuine_new = len(orders_after - orders_before)
        persistent = len(orders_before & orders_after)

        lob.set_total_orders_added(persistent + genuine_new)
        lob.set_total_orders_cancelled(genuine_cancels)

        best_bid = lob.get_best_bid()
        best_ask = lob.get_best_ask()

        if lob.is_empty() or best_bid <= 0 or best_ask <= 0:
            continue

        features = extract_features(lob)

        label = int(event.get("label", 0))

        rows.append([
            features[0],
            features[1],
            features[2],
            features[3],
            features[4],
            label
        ])

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "spread",
            "bid_vol",
            "ask_vol",
            "imbalance",
            "cancel_rate",
            "label"
        ])
        writer.writerows(rows)

    total = len(rows)
    positives = sum(1 for r in rows if r[-1] == 1)
    negatives = total - positives

    print("=" * 70)
    print("Feature dataset created")
    print("=" * 70)
    print("Input replay:", resolve_path(input_json))
    print("Output CSV:", output_csv)
    print("Total rows:", total)
    print("Normal label=0:", negatives)
    print("Spoofing label=1:", positives)

    if total > 0:
        print("Spoofing ratio:", round(positives / total * 100, 4), "%")

    print("=" * 70)

def main():
    parser = argparse.ArgumentParser(description="Build ML feature CSV from FI-2010 replay JSON")
    parser.add_argument("--input", required=True, help="Input replay JSON")
    parser.add_argument("--output", required=True, help="Output feature CSV")

    args = parser.parse_args()

    replay_to_features(
        input_json=args.input,
        output_csv=args.output
    )

if __name__ == "__main__":
    main()