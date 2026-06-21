import argparse
import json
import os
import numpy as np


def load_fi2010_file(path):
    try:
        data = np.genfromtxt(path, delimiter=",")
        if np.isnan(data).all():
            raise ValueError("comma delimiter failed")
    except Exception:
        data = np.genfromtxt(path)

    if data.ndim != 2:
        raise ValueError(f"Expected 2D matrix, got shape={data.shape}")

    return data


def ensure_samples_by_features(data):
    rows, cols = data.shape

    if rows in [40, 144, 145, 149] and cols > rows:
        print("[Convert] Detected FEATURES x SAMPLES format. Transposing...")
        data = data.T
    else:
        print("[Convert] Data seems already SAMPLES x FEATURES.")

    if data.shape[1] < 40:
        raise ValueError(f"Need at least 40 LOB features, got shape={data.shape}")

    return data


def row_to_depth_event(row, timestamp_ms, price_scale=1000.0, volume_scale=1000000.0):
    lob_features = row[:40]

    bids = []
    asks = []

    for level in range(10):
        idx = level * 4

        ask_price = float(lob_features[idx]) * price_scale
        ask_volume = float(lob_features[idx + 1]) * volume_scale
        bid_price = float(lob_features[idx + 2]) * price_scale
        bid_volume = float(lob_features[idx + 3]) * volume_scale

        if bid_price > 0 and bid_volume > 0:
            bids.append([f"{bid_price:.6f}", f"{bid_volume:.6f}"])

        if ask_price > 0 and ask_volume > 0:
            asks.append([f"{ask_price:.6f}", f"{ask_volume:.6f}"])

    return {
        "e": "depthUpdate",
        "E": int(timestamp_ms),
        "b": bids,
        "a": asks
    }


def convert(input_path, output_path, start_row=0, max_rows=200):
    data = load_fi2010_file(input_path)
    print("[Convert] Original shape:", data.shape)

    data = ensure_samples_by_features(data)
    print("[Convert] Normalized shape:", data.shape)

    end_row = min(start_row + max_rows, data.shape[0])
    selected = data[start_row:end_row]

    events = []
    base_timestamp = 1718800000000

    for i, row in enumerate(selected):
        event = row_to_depth_event(
            row=row,
            timestamp_ms=base_timestamp + i * 100
        )

        if event["b"] and event["a"]:
            events.append(event)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

    print("=" * 70)
    print("FI-2010 DecPre conversion completed")
    print("=" * 70)
    print("Input:", input_path)
    print("Output:", output_path)
    print("Selected rows:", len(selected))
    print("Saved events:", len(events))

    if events:
        first_event = events[0]
        print("\nFirst converted event:")
        print(json.dumps(first_event, indent=2)[:1000])

        best_bid = max(float(x[0]) for x in first_event["b"])
        best_ask = min(float(x[0]) for x in first_event["a"])
        print("\nSanity check:")
        print("Best bid:", best_bid)
        print("Best ask:", best_ask)
        print("Spread:", best_ask - best_bid)

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to FI-2010 DecPre file")
    parser.add_argument("--output", required=True, help="Output replay JSON file")
    parser.add_argument("--start-row", type=int, default=0)
    parser.add_argument("--max-rows", type=int, default=200)

    args = parser.parse_args()

    convert(
        input_path=args.input,
        output_path=args.output,
        start_row=args.start_row,
        max_rows=args.max_rows
    )


if __name__ == "__main__":
    main()