import sys
import os
import logging

src_dir = os.path.dirname(os.path.abspath(__file__))
build_dirs = [
    os.path.normpath(os.path.join(src_dir, '../../build')),
    os.path.normpath(os.path.join(src_dir, '../../build-py311-v2')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc-nmake')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc/Debug')),
    os.path.normpath(os.path.join(src_dir, '../../build-msvc/Release')),
]
for build_dir in build_dirs:
    if build_dir not in sys.path:
        sys.path.insert(0, build_dir)

logger = logging.getLogger(__name__)

try:
    import lob_core
except ImportError as e:
    logger.error("Không tìm thấy thư viện C++. Hãy chạy CMake trước. Chi tiết: %s", e)
    lob_core = None

def run_evaluation(lob_state):
    """
    Hàm chính để đánh giá điểm Utility cho 1 state. 
    Gọi trực tiếp thuật toán Alpha-Beta & Q-Search chạy bằng mã máy C++ 
    để đạt hiệu năng tối đa (không tốn overhead gọi đệ quy trong Python).
    """
    if lob_core is None:
        raise RuntimeError("lob_core C++ module not available — cannot evaluate state")
    return lob_state.alpha_beta_search(3, -1e18, 1e18, True)
