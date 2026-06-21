# Fraud Detection-Use Case: Detecting Fraudulent Transactions in Financial Systems, using Chess-based algorithm with small ML model to detect Spoofing in trading with Spark and Kafka

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
BigData/
├── CMakeLists.txt
├── docker-compose.yml
├── requirements.txt
├── README.md
├── data/
│   └── replay/
├── models/
│   └── spoofing_rf_model.pkl

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
│       └── output_recorder.py
└── tools/
    └── winutils_stub.c
```

## Yêu Cầu Môi Trường

Khuyến nghị trên Windows:

- Python 3.11 64-bit
- Java 17
- CMake
- MSVC x64 Build Tools / Visual Studio C++ toolchain
- Docker Desktop nếu chạy Redis bằng Docker
- Redis nếu chạy streaming/replay

Python 3.12 có thể chạy một số phần, nhưng PySpark worker trên Windows dễ lỗi hơn. Project hiện khuyến nghị dùng Python 3.11.

## Cài Thư Viện Python

```powershell
cd path\to\BigDataSang
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

Các thư mục build là artifact local, không cần commit lên Git.

## Chạy Demo Tĩnh

Demo tĩnh chạy toàn bộ pipeline từ LOB giả lập đến ML, Spark, Shapley và kết luận cuối.

```powershell
cd path\to\BigDataSang
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

`10_final_result.json` là kết luận cuối cùng của pipeline.

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

```powershell
docker compose up -d
```

### Terminal 1: Spark Streaming Engine

```powershell
cd path\to\BigDataSang
$env:HADOOP_HOME="$PWD\hadoop"
$env:hadoop_home=$env:HADOOP_HOME
$env:Path="$env:HADOOP_HOME\bin;$env:Path"
$env:PYTHONIOENCODING="utf-8"
$env:PYSPARK_PYTHON=(py -3.11 -c "import sys; print(sys.executable)")
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
& $env:PYSPARK_PYTHON src/python/streaming_engine.py
```

### Terminal 2: Replay Data Vào Redis

```powershell
cd path\to\BigDataSang
py -3.11 src/python/replay_manipulation.py
```

## Train ML Model

Model đã train được lưu tại:

```text
models/spoofing_rf_model.pkl
```

Nếu cần train lại:

```powershell
cd path\to\BigDataSang
py -3.11 src/python/train_ml_model.py
```

Metric mẫu:

```text
accuracy  = 0.9673
precision = 0.3867
recall    = 0.5079
f1        = 0.4391
roc_auc   = 0.8109
```

## FI-2010 / Replay Dataset

Project có các script hỗ trợ chuẩn bị dữ liệu:

- `build_fi2010_ml_dataset.py`: tạo dataset huấn luyện ML từ FI-2010.
- `convert_fi2010_to_replay.py`: chuyển dữ liệu FI-2010 sang replay JSON.
- `inspect_fi2010_decpre.py`: kiểm tra định dạng dữ liệu FI-2010.

Các file replay nên đặt trong:

```text
data/replay/
```

## Ghi Chú Git

Không commit các thư mục sinh ra khi chạy:

```text
build/
build-*/
outputs/
hadoop/
__pycache__/
```

Nếu chỉ muốn push replay data, nên track:

```text
data/replay/**
```

và tránh add các file data lớn/gốc ngoài `data/replay`.

## Lỗi Thường Gặp

### Docker không kết nối được daemon

Mở Docker Desktop, đợi engine chạy, rồi chạy lại:

```powershell
docker compose up -d
```

### Java lỗi `getSubject is not supported`

Nguyên nhân thường là dùng Java quá mới như Java 24. Dùng Java 17:

```powershell
java -version
```

### PySpark worker crash trên Windows

Khuyến nghị dùng Python 3.11 và set:

```powershell
$env:PYSPARK_PYTHON=(py -3.11 -c "import sys; print(sys.executable)")
$env:PYSPARK_DRIVER_PYTHON=$env:PYSPARK_PYTHON
```

### HADOOP_HOME unset

```powershell
$env:HADOOP_HOME="$PWD\hadoop"
$env:hadoop_home=$env:HADOOP_HOME
$env:Path="$env:HADOOP_HOME\bin;$env:Path"
```

### UnicodeEncodeError khi in tiếng Việt

```powershell
$env:PYTHONIOENCODING="utf-8"
```

## Kết Luận

Project minh họa một pipeline phát hiện thao túng thị trường tương đối đầy đủ: từ mô phỏng sổ lệnh, lọc nhanh bằng ML, phân tích rủi ro bằng C++/Spark, đến phát hiện cộng đồng giao dịch chéo và định danh ringleader bằng Shapley Value. Kết quả thực nghiệm được lưu thành artifact để dễ kiểm chứng và đưa vào báo cáo.
