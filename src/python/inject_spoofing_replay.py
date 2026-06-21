import argparse
import json
import os
import random


def resolve_path(path):
    if os.path.isabs(path):
        return path

    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(os.path.join(src_dir, "../.."))
    return os.path.normpath(os.path.join(project_root, path))


def load_events(path):
    with open(path, "r", encoding="utf-8") as f:
        events = json.load(f)

    if not isinstance(events, list):
        raise ValueError("Input replay JSON must be a list of events.")

    return events


def avg_volume(levels):
    volumes = []

    for item in levels:
        if len(item) < 2:
            continue

        try:
            v = float(item[1])
            if v > 0:
                volumes.append(v)
        except Exception:
            pass

    if not volumes:
        return 1.0

    return sum(volumes) / len(volumes)


def inject_spoofing(
    events,
    episodes,
    min_len,
    max_len,
    min_mult,
    max_mult,
    seed
):
    random.seed(seed)

    for event in events:
        event["label"] = 0
        event["spoofing_injected"] = False

    n = len(events)
    used_ranges = []
    injected_event_count = 0

    def is_overlap(start, end):
        for s, e in used_ranges:
            if not (end < s or start > e):
                return True
        return False

    for ep_id in range(1, episodes + 1):
        length = random.randint(min_len, max_len)

        max_start = n - length - 5
        if max_start <= 10:
            break

        start = None

        for _ in range(300):
            candidate = random.randint(10, max_start)
            if not is_overlap(candidate, candidate + length):
                start = candidate
                break

        if start is None:
            continue

        end = start + length
        used_ranges.append((start, end))

        side = random.choice(["BID", "ASK"])
        key = "b" if side == "BID" else "a"

        multiplier = random.uniform(min_mult, max_mult)

        for idx in range(start, end):
            event = events[idx]

            if key not in event or not event[key]:
                continue

            levels = event[key]
            base_avg_vol = avg_volume(levels)
            spoof_volume = base_avg_vol * multiplier

            price = levels[0][0]

            if idx >= end - 2:
                levels[0] = [price, "0.000000"]
                phase = "cancel"
            else:
                levels[0] = [price, f"{spoof_volume:.6f}"]
                phase = "spoof_wall"

            event["label"] = 1
            event["spoofing_injected"] = True
            event["spoofing_episode_id"] = ep_id
            event["spoofing_side"] = side
            event["spoofing_phase"] = phase
            event["spoofing_multiplier"] = round(multiplier, 4)

            injected_event_count += 1

    return events, injected_event_count, used_ranges


def main():
    parser = argparse.ArgumentParser(description="Inject spoofing patterns into FI-2010 replay JSON")
    parser.add_argument("--input", required=True, help="Input normal replay JSON")
    parser.add_argument("--output", required=True, help="Output spoofing replay JSON")
    parser.add_argument("--episodes", type=int, default=100)
    parser.add_argument("--min-len", type=int, default=10)
    parser.add_argument("--max-len", type=int, default=20)
    parser.add_argument("--min-mult", type=float, default=8.0)
    parser.add_argument("--max-mult", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)

    events = load_events(input_path)

    injected_events, injected_count, ranges = inject_spoofing(
        events=events,
        episodes=args.episodes,
        min_len=args.min_len,
        max_len=args.max_len,
        min_mult=args.min_mult,
        max_mult=args.max_mult,
        seed=args.seed
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(injected_events, f, indent=2)

    total = len(injected_events)
    label_1 = sum(1 for e in injected_events if e.get("label") == 1)
    label_0 = total - label_1

    print("=" * 70)
    print("Spoofing injection completed")
    print("=" * 70)
    print("Input:", input_path)
    print("Output:", output_path)
    print("Total events:", total)
    print("Normal events label=0:", label_0)
    print("Spoofing events label=1:", label_1)
    print("Requested episodes:", args.episodes)
    print("Actual injected episodes:", len(ranges))
    print("Spoofing ratio:", round(label_1 / total * 100, 4), "%")
    print("=" * 70)


if __name__ == "__main__":
    main()