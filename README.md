# Market Manipulation Detection System: Spoofing and Wash Trading Detection with C++ Limit Order Book, Spark, and Game-Theory Search

Hệ thống phát hiện thao túng thị trường tài chính theo hướng hybrid analytics: kết hợp lõi C++ tốc độ cao, Machine Learning, Apache Spark và phân tích đồ thị để phát hiện spoofing, wash trading và xác định tài khoản giữ vai trò trung tâm trong cụm giao dịch nghi vấn.

Project có hai chế độ chạy chính:

- **Demo tĩnh**: chạy toàn bộ pipeline từ trạng thái sổ lệnh giả lập và lưu kết quả vào `outputs/`
- **Streaming replay**: đẩy dữ liệu replay vào Redis Stream, Spark Structured Streaming đọc và phân tích theo micro-batch

## Kiến Trúc Tổng Quan

```text
Data / Replay / Binance
        |
        v
Redis Stream / Static Demo
        |
        v
Python Orchestrator
        |
        +-- Layer 1: ML Fast Filter
        +-- Layer 2: C++ LOB + Alpha-Beta Search
        +-- Layer 3: Spark RDD + Spark SQL
        +-- Layer 4: Graph Analytics + Shapley Value
        |
        v
Output Redis Alerts
```

## Cấu Trúc Thư Mục

```text
BigDataProject/
├── CMakeLists.txt
├── docker-compose.yml
├── requirements.txt
├── README.md
├── run_pipeline.sh
├── streaming_demo.ipynb
├── data/
│   └── replay/
├── models/
│   ├── spoofing_rf_model.pkl
│   └── spark_ml_rf/
├── outputs/
│   └── <run_id>/
├── src/
│   ├── cpp/
│   │   ├── lob.h
│   │   ├── lob.cpp
│   │   └── bindings.cpp
│   └── python/
│       ├── main.py
│       ├── streaming_engine.py
│       ├── replay_manipulation.py
│       ├── ml_filter.py
│       ├── train_ml_model.py
│       ├── build_fi2010_ml_dataset.py
│       ├── convert_fi2010_to_replay.py
│       ├── spark_engine.py
│       ├── shapley_analyzer.py
│       ├── redis_manager.py
│       ├── output_recorder.py
│       ├── binance_ws.py
│       ├── inject_spoofing_replay.py
│       └── train_spark_ml_model.py
    
```

## Yêu Cầu Môi Trường

Khuyến nghị trên Windows:

- Python 3.11 64-bit
- Java 17
- CMake
- MSVC x64 Build Tools / Visual Studio C++ toolchain
- Docker Desktop nếu chạy Redis bằng Docker
- Redis nếu chạy streaming/replay

Python 3.12 có thể chạy một số phần, nhưng PySpark worker trên Windows dễ lỗi hơn. Project hiện khuyến nghị dùng Python 3.11

## Cài Thư Viện Python

```powershell
cd path\to\BigDataProject
py -3.11 -m pip install -r requirements.txt
```

Kiểm tra nhanh:

```powershell
py -3.11 -c "import pyspark, sklearn, numpy, networkx, redis; print('ok')"
```

## Build C++ Core

Python không import trực tiếp được file `.cpp`. Cần build C++ thành module Python:

- Python 3.11: `lob_core.cp311-win_amd64.pyd`
- Python 3.12: `lob_core.cp312-win_amd64.pyd`

Lệnh build khuyến nghị cho Python 3.11 trên Windows:

```powershell
$env:PYTHON_EXE=(py -3.11 -c "import sys; print(sys.executable)")
cmake -S . -B build-py311-v2 -G "NMake Makefiles" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DPYTHON_EXECUTABLE="$env:PYTHON_EXE"
cmake --build build-py311-v2
```

Sau khi build thành công, cần có:

```text
build-py311-v2/lob_core.cp311-win_amd64.pyd
```

## Chạy Demo Tĩnh

Demo tĩnh chạy toàn bộ pipeline từ LOB giả lập đến ML, Spark, Shapley và kết luận cuối

```powershell
cd path\to\BigDataProject
$env:PYTHONIOENCODING="utf-8"
$env:PYSPARK_PYTHON=(py -3.11 -c "import sys; print(sys.executable)")
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
& $env:PYSPARK_PYTHON src/python/main.py
```

Sau khi chạy, kết quả được lưu tại:

```text
outputs/<run_id>/
```

Các file output quan trọng:

```text
01_initial_lob_snapshot.json
02_ml_filter_result.json
03_scenario_results.csv
04_spark_summary.json
05_top5_scenarios.json
05_top5_scenarios.csv
06_wash_trading_community.json
07_shapley_values.json
07_shapley_values.csv
10_final_result.json
manifest.json
```

`10_final_result.json` là kết luận cuối cùng của pipeline

## Kết Quả Demo Mẫu

Từ run `20260620_083945`, hệ thống ghi nhận:

```text
ML probability: 1.0
Total scenarios: 200
Average risk: 149.26
Max risk: 151.67
P95 risk: 150.12
Final status: SPOOFING_AND_WASH_TRADING
Severity: CRITICAL
Ringleader: ACC_RINGLEADER
```

Kết quả Shapley:

```text
ACC_RINGLEADER  453.416667
ACC_3           273.533333
ACC_1           149.966667
ACC_2           123.083333
```

## Chạy Streaming / Replay

Streaming mode cần Redis. Cách đơn giản là chạy Redis bằng Docker:

### Terminal 1: Chạy redis

```powershell
docker compose up -d
```

### Terminal 2: Spark Streaming Engine

```powershell
cd path\to\BigDataProject
$env:HADOOP_HOME="$PWD\hadoop"
$env:hadoop_home=$env:HADOOP_HOME
$env:Path="$env:HADOOP_HOME\bin;$env:Path"
$env:PYTHONIOENCODING="utf-8"
$env:PYSPARK_PYTHON=(py -3.11 -c "import sys; print(sys.executable)")
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
& $env:PYSPARK_PYTHON src/python/streaming_engine.py
```

### Terminal 3: Replay Data Vào Redis

```powershell
cd path\to\BigDataProject
py -3.11 src/python/replay_manipulation.py --file data/replay/test_spoofing_replay.json --sleep 0.1
```

## Train ML Model

Model đã train được lưu tại:

```text
models/spoofing_rf_model.pkl
```

Nếu cần train lại:

```powershell
cd path\to\BigDataProject
py -3.11 src/python/train_ml_model.py
```

Metric mẫu:

```text
train samples        = 50,000
test samples         = 10,000
test spoofing ratio  = 2.4%
accuracy             = 0.8461
precision            = 0.1177
recall               = 0.8333
f1                   = 0.2063
roc_auc              = 0.9120
confusion matrix     = [[8261, 1499], [40, 200]]
```

## Train Spark ML Model

Spark MLlib RandomForest model được lưu tại:

```text
models/spark_ml_rf/
```

Train lại Spark ML model:

```powershell
cd path\to\BigDataProject
py -3.11 src/python/train_spark_ml_model.py --num-trees 20 --max-depth 6
```

Metrics Spark ML sau khi train lại:

```text
train samples        = 50,000
test samples         = 10,000
num_trees            = 20
max_depth            = 6
train seconds        = 4.57
accuracy             = 0.7090
weighted_precision   = 0.9750
weighted_recall      = 0.7090
f1                   = 0.8086
roc_auc              = 0.9107
confusion matrix     = tn=6869, fp=2891, fn=19, tp=221
```

## FI-2010 / Replay Dataset

Project có các script hỗ trợ chuẩn bị dữ liệu từ bộ FI-2010 và chuyển đổi sang dạng replay để phục vụ mô phỏng luồng sự kiện:

- `build_fi2010_ml_dataset.py`: tạo dataset huấn luyện ML từ FI-2010
- `convert_fi2010_to_replay.py`: chuyển dữ liệu FI-2010 sang replay JSON
- `inject_spoofing_replay.py`: chèn các spoofing episode vào replay normal để tạo dữ liệu có label

Các file replay đặt trong:

```text
data/replay/
```

Bộ replay sử dụng trong project gồm 4 file chính:

```text
data/replay/train_normal_replay.json
data/replay/train_spoofing_replay.json
data/replay/test_normal_replay.json
data/replay/test_spoofing_replay.json
```

Train replay lấy 50,000 event từ file train FI-2010. Test replay đã được tạo lại từ file test riêng để tránh trùng dữ liệu train:

```text
data/raw/fi2010/test/Test_Dst_NoAuction_DecPre_CF_7.txt
```
## Notebook Demo

Notebook `streaming_demo.ipynb` được dùng để chạy demo streaming theo dạng có lưu log:

- khởi động Redis
- chạy `streaming_engine.py` ở background
- replay dữ liệu vào Redis Stream
- lưu log vào `outputs/streaming_demo_log.txt` và `outputs/replay_demo_log.txt`

Các log này dùng để kiểm tra và trình bày kết quả demo

## Kết Luận

Project minh họa một pipeline phát hiện thao túng thị trường tương đối đầy đủ: từ mô phỏng sổ lệnh, lọc nhanh bằng ML, phân tích rủi ro bằng C++/Spark, đến phát hiện cộng đồng giao dịch chéo và định danh ringleader bằng Shapley Value

Các kết quả thực nghiệm được lưu trong thư mục outputs/ để phục vụ kiểm tra, đánh giá và demo hệ thống