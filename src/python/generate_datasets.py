import os
import csv
import json
import random

def generate_synthetic_csv(file_path):
    print(f"Generating synthetic LOB data at {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    header = ['spread', 'bid_vol', 'ask_vol', 'imbalance', 'cancel_rate', 'label']
    
    rng = random.Random(2026)
    
    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        
        for _ in range(4000):
            spread = rng.uniform(0.01, 2.0)
            bid_vol = rng.uniform(1, 500)
            ask_vol = rng.uniform(1, 500)
            total = bid_vol + ask_vol
            imbalance = abs((bid_vol - ask_vol) / total) if total > 0 else 0.0
            cancel_rate = rng.uniform(0.0, 0.15)
            writer.writerow([
                round(spread, 4), 
                round(bid_vol, 2), 
                round(ask_vol, 2), 
                round(imbalance, 4), 
                round(cancel_rate, 4), 
                0
            ])
            
        for _ in range(1000):
            spread = rng.uniform(0.01, 15.0)
            bid_vol = rng.uniform(5000, 50000)
            ask_vol = rng.uniform(1, 150)
            total = bid_vol + ask_vol
            imbalance = abs((bid_vol - ask_vol) / total) if total > 0 else 0.0
            cancel_rate = rng.uniform(0.0, 0.95)
            writer.writerow([
                round(spread, 4), 
                round(bid_vol, 2), 
                round(ask_vol, 2), 
                round(imbalance, 4), 
                round(cancel_rate, 4), 
                1
            ])
            
    print("Synthetic CSV generation complete!")


def generate_classic_spoofing_json(file_path):
    print(f"Generating classic spoofing case simulation data at {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    events = []
    base_time = 1718800000000
    
    for i in range(10):
        events.append({
            "e": "depthUpdate",
            "E": base_time + i * 100,
            "b": [["100.00", "5.2"], ["99.50", "12.0"], ["99.00", "20.0"]],
            "a": [["100.50", "4.8"], ["101.00", "15.0"], ["101.50", "25.0"]]
        })
        
    for i in range(10):
        events.append({
            "e": "depthUpdate",
            "E": base_time + (10 + i) * 100,
            "b": [
                ["100.20", "6.5"], 
                ["99.80", "500.0"],
                ["99.50", "12.0"]
            ],
            "a": [["100.60", "3.0"], ["101.00", "10.0"], ["101.50", "15.0"]]
        })

    for i in range(5):
        events.append({
            "e": "depthUpdate",
            "E": base_time + (20 + i) * 100,
            "b": [
                ["100.50", "8.0"],
                ["99.80", "500.0"],
                ["99.50", "12.0"]
            ],
            "a": [["102.00", "1.5"], ["102.50", "8.0"], ["103.00", "10.0"]]
        })

    for i in range(5):
        events.append({
            "e": "depthUpdate",
            "E": base_time + (25 + i) * 100,
            "b": [
                ["99.00", "1.0"],
                ["98.00", "10.0"],
                ["99.80", "0.0"]
            ],
            "a": [["100.10", "40.0"], ["100.50", "50.0"]]
        })
        
    with open(file_path, mode='w', encoding='utf-8') as f:
        json.dump(events, f, indent=2)
        
    print("Classic spoofing JSON simulation data complete!")

if __name__ == "__main__":
    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.normpath(os.path.join(src_dir, '../..'))
    
    csv_file = os.path.join(project_root, 'data/synthetic_lob_data.csv')
    json_file = os.path.join(project_root, 'data/classic_spoofing_case.json')
    
    generate_synthetic_csv(csv_file)
    generate_classic_spoofing_json(json_file)
