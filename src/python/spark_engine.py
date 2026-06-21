"""
Spark Engine — Lõi xử lý phân tán (Tầng 2 & 3).
Sử dụng Spark RDD + Broadcast Variables để phân phối kịch bản C++ Alpha-Beta
trên toàn bộ CPU cores thông qua cơ chế Map-Reduce.
"""
import sys
import os
import time
import random
import logging
import threading
from collections import defaultdict

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_BUILD_DIRS = [
    os.path.normpath(os.path.join(_SRC_DIR, '../../build')),
    os.path.normpath(os.path.join(_SRC_DIR, '../../build-py311-v2')),
    os.path.normpath(os.path.join(_SRC_DIR, '../../build-msvc-nmake')),
    os.path.normpath(os.path.join(_SRC_DIR, '../../build-msvc/Debug')),
    os.path.normpath(os.path.join(_SRC_DIR, '../../build-msvc/Release')),
]

for _build_dir in _BUILD_DIRS:
    if _build_dir not in sys.path:
        sys.path.insert(0, _build_dir)

logger = logging.getLogger("SparkEngine")
_clone_lock = threading.Lock()


def _ensure_worker_paths():
    """Đảm bảo Spark worker có thể import lob_core (cần cho unpickle LOB objects)."""
    import sys as _s
    for p in _BUILD_DIRS + [_SRC_DIR]:
        if p not in _s.path:
            _s.path.insert(0, p)

def _evaluate_scenario(args):
    """
    Hàm MAP chạy trên mỗi Spark Worker.
    Tạo 1 kịch bản thị trường có nhiễu ngẫu nhiên,
    sau đó chạy thuật toán C++ Alpha-Beta Search.
    
    Input:  (scenario_id, base_state)  — base_state từ Broadcast variable
    Output: dict chứa kết quả phân tích
    """
    _ensure_worker_paths()
    import lob_core as _lob
    
    scenario_id, base_state = args
    
    with _clone_lock:
        s = base_state.clone()
    
    rng = random.Random(42 + scenario_id)
    
    market_vol = rng.randint(500, 2000)
    s.set_market_order_volume(market_vol)
    
    noise_id_base = 9000000000 + scenario_id * 100
    num_noise = rng.randint(1, 5)
    actual_noise = 0
    
    for j in range(num_noise):
        side = rng.choice([_lob.Side.BID, _lob.Side.ASK])
        best_bid = s.get_best_bid()
        best_ask = s.get_best_ask()
        
        if side == _lob.Side.BID:
            bid_min = best_bid * 0.95 if best_bid > 0.0 else 95.0
            bid_max = best_bid if best_bid > 0.0 else 100.0
            price = round(rng.uniform(bid_min, bid_max), 1)
        else:
            ask_min = best_ask if best_ask > 0.0 else 110.0
            ask_max = best_ask * 1.05 if best_ask > 0.0 else 115.0
            price = round(rng.uniform(ask_min, ask_max), 1)
            
        volume = rng.randint(10, 500)
        
        if side == _lob.Side.BID and best_ask > 0 and price >= best_ask:
            continue
        if side == _lob.Side.ASK and best_bid > 0 and price <= best_bid:
            continue
            
        s.add_order(noise_id_base + j, side, price, volume, 2000 + scenario_id)
        actual_noise += 1

    risk_score = s.alpha_beta_search(3, -1e18, 1e18, True)
    
    return {
        "scenario_id": scenario_id,
        "risk_score": float(risk_score),
        "market_volume": int(market_vol),
        "noise_orders": actual_noise
    }


def analyze_scenarios_distributed(spark, base_state, num_scenarios=200):
    sc = spark.sparkContext
    num_cores = sc.defaultParallelism
    
    logger.info(f"[SparkEngine] Broadcast LOB state tới {num_cores} workers...")
    base_broadcast = sc.broadcast(base_state)
    
    logger.info(f"[SparkEngine] Phân phối {num_scenarios} kịch bản qua Spark RDD (local[*])...")
    start_time = time.time()
    
    scenario_rdd = sc.parallelize(range(num_scenarios), numSlices=num_cores) \
                     .map(lambda sid: (sid, base_broadcast.value))

    results_rdd = scenario_rdd.map(_evaluate_scenario)

    results = results_rdd.collect()
    elapsed = time.time() - start_time

    base_broadcast.unpersist()
    
    results_df = spark.createDataFrame(results)
    results_df.createOrReplaceTempView("scenario_results")
    
    summary_df = spark.sql("""
        SELECT 
            COUNT(*)                    AS total_scenarios,
            ROUND(AVG(risk_score), 2)   AS avg_risk,
            ROUND(MAX(risk_score), 2)   AS max_risk,
            ROUND(MIN(risk_score), 2)   AS min_risk,
            ROUND(STDDEV(risk_score),2) AS stddev_risk,
            ROUND(percentile_approx(risk_score, 0.95), 2) AS p95_risk
        FROM scenario_results
    """)
    
    summary_row = summary_df.collect()[0]
    summary = {
        "total_scenarios": summary_row["total_scenarios"],
        "avg_risk": summary_row["avg_risk"],
        "max_risk": summary_row["max_risk"],
        "min_risk": summary_row["min_risk"],
        "stddev_risk": summary_row["stddev_risk"],
        "p95_risk": summary_row["p95_risk"],
        "elapsed_seconds": round(elapsed, 4)
    }
    
    logger.info(f"[SparkEngine] Hoàn thành trong {elapsed:.4f}s. Avg Risk: {summary['avg_risk']}, Max Risk: {summary['max_risk']}")
    
    return results_df, summary

def _compute_single_permutation(args):
    """
    Hàm MAP cho Shapley: Tính marginal contribution của 1 hoán vị.
    Chạy trên Spark Worker.
    """
    perm_id, accounts_list, transactions = args
    
    rng = random.Random(perm_id)
    perm = accounts_list.copy()
    rng.shuffle(perm)
    
    contributions = {}
    coalition = set()
    current_val = 0.0
    
    for acc in perm:
        coalition.add(acc)
        internal_vol = 0.0
        for buyer, seller, volume in transactions:
            if buyer in coalition and seller in coalition:
                internal_vol += volume
        
        marginal = internal_vol - current_val
        contributions[acc] = marginal
        current_val = internal_vol
    
    return contributions


def shapley_spark(spark, accounts, transactions, num_permutations=3000):
    sc = spark.sparkContext
    accounts_list = sorted(list(accounts))
    
    if len(accounts_list) <= 1:
        if accounts_list:
            val = sum(v for b, s, v in transactions if b in accounts and s in accounts)
            return spark.createDataFrame([{"account": accounts_list[0], "shapley_value": float(val)}])
        return spark.createDataFrame([], "account STRING, shapley_value DOUBLE")
    
    logger.info(f"[SparkShapley] Phân tán {num_permutations} hoán vị qua Spark RDD...")
    start_time = time.time()
    
    accounts_bc = sc.broadcast(accounts_list)
    transactions_bc = sc.broadcast(transactions)
    
    perm_rdd = sc.parallelize(range(num_permutations), numSlices=sc.defaultParallelism)
    
    contributions_rdd = perm_rdd.map(
        lambda pid: _compute_single_permutation((pid, accounts_bc.value, transactions_bc.value))
    )

    all_contributions = contributions_rdd.collect()
    
    shapley_values = defaultdict(float)
    for c in all_contributions:
        for acc, val in c.items():
            shapley_values[acc] += val / num_permutations

    accounts_bc.unpersist()
    transactions_bc.unpersist()
    
    elapsed = time.time() - start_time
    logger.info(f"[SparkShapley] Hoàn thành trong {elapsed:.4f}s.")
    
    shapley_rows = [{"account": acc, "shapley_value": round(val, 6)} 
                    for acc, val in shapley_values.items()]
    shapley_df = spark.createDataFrame(shapley_rows).orderBy("shapley_value", ascending=False)
    
    return shapley_df
