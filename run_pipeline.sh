GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

CONDA_ENV="${CONDA_ENV:-ai_env}"

echo -e "${GREEN}=================================================${NC}"
echo -e "${GREEN} KHỞI ĐỘNG HỆ THỐNG GIÁM SÁT THỊ TRƯỜNG BINANCE  ${NC}"
echo -e "${GREEN}=================================================${NC}"
echo -e "${CYAN}Conda environment: ${CONDA_ENV}${NC}"

mkdir -p /tmp/binance_stream
rm -f /tmp/binance_stream/*

export PYTHONUNBUFFERED=1
PYTHON_CMD=(conda run --no-capture-output -n "${CONDA_ENV}" python -u)

cleanup() {
    if [ -n "$WS_PID" ]; then
        echo -e "${CYAN}Đang dừng tiến trình WebSocket dưới nền (PID: $WS_PID)...${NC}"
        kill $WS_PID 2>/dev/null
        wait $WS_PID 2>/dev/null
        echo -e "${GREEN}Đã tắt toàn bộ hệ thống an toàn.${NC}"
    fi
}
trap cleanup EXIT

echo -e "${CYAN}[1/2] Đang khởi động Vòi dữ liệu Binance (binance_ws.py) dưới nền...${NC}"
"${PYTHON_CMD[@]}" src/python/binance_ws.py &
WS_PID=$!

sleep 3

echo -e "${CYAN}[2/2] Đang khởi động Động cơ Phân tích Spark (streaming_engine.py)...${NC}"
"${PYTHON_CMD[@]}" src/python/streaming_engine.py