import random
import networkx as nx
from collections import defaultdict

def detect_wash_trading_communities(transactions):
    """
    TẬP HỢP NÂNG CẤP TẦNG 4: Graph Analytics cho Wash Trading Network.
    Sử dụng NetworkX để dựng đồ thị có hướng biểu diễn dòng giao dịch.
    Tìm kiếm các nhóm tài khoản giao dịch chéo (Wash Trading) dựa trên 
    các Thành phần Liên thông Mạnh (Strongly Connected Components - SCC)
    có kích thước lớn hơn hoặc bằng 2.
    
    Đầu vào: list of tuples: (buyer, seller, volume)
    Đầu ra: list of lists: Các nhóm tài khoản giao dịch chéo nghi ngờ
    """
    if not transactions:
        print("[GraphAnalyzer] Không có giao dịch nào để phân tích.")
        return []
    
    print("[GraphAnalyzer] Dựng đồ thị có hướng giao dịch bằng NetworkX...")
    G = nx.DiGraph()
    for buyer, seller, volume in transactions:
        if G.has_edge(buyer, seller):
            G[buyer][seller]['volume'] += volume
            G[buyer][seller]['count'] += 1
        else:
            G.add_edge(buyer, seller, volume=volume, count=1)
            
    sccs = list(nx.strongly_connected_components(G))
    communities = [
        sorted(list(scc)) for scc in sccs 
        if len(scc) >= 2 or (len(scc) == 1 and G.has_edge(list(scc)[0], list(scc)[0]))
    ]

    communities.sort(key=lambda c: _internal_volume(c, G), reverse=True)
    
    print(f"[GraphAnalyzer] Đã tìm thấy {len(communities)} cụm liên thông mạnh nghi ngờ giao dịch chéo.")
    for i, comm in enumerate(communities):
        vol = _internal_volume(comm, G)
        print(f"  Cụm {i+1}: {comm} — Tổng volume nội bộ: {vol:,.0f}")
    return communities

def _internal_volume(community, G):
    """Tính tổng volume giao dịch nội bộ trong một cụm."""
    vol = 0
    comm_set = set(community)
    for u in community:
        for v in G.successors(u):
            if v in comm_set:
                vol += G[u][v].get('volume', 0)
    return vol

def evaluate_coalition_liquidity(coalition, transactions):
    """
    Tính lượng thanh khoản ảo (wash volume) thực tế do một liên minh (coalition) tạo ra.
    Chỉ đếm các giao dịch chéo nội bộ giữa các thành viên trong liên minh.
    Đây là hàm đánh giá mang tính thực tế cao (thay cho hàm dummy cũ).
    
    Coalition is stored as a set for constant-time membership checks.
    Self-loop handling is naturally correct because buyer == seller is in coalition_set.
    """
    coalition_set = set(coalition) if not isinstance(coalition, set) else coalition
    internal_volume = 0.0
    for buyer, seller, volume in transactions:
        if buyer in coalition_set and seller in coalition_set:
            internal_volume += volume
    return internal_volume

def monte_carlo_shapley(accounts, transactions, num_permutations=1000):
    """
    Tính Shapley Value xấp xỉ bằng Monte Carlo cho một nhóm cụm tài khoản (Tầng 4).
    
    Cải tiến:
    - Dùng list copy cố định thay vì tạo mới mỗi vòng lặp
    - Xử lý trường hợp edge-case: nhóm 1 phần tử, nhóm rỗng
    """
    if not accounts:
        print("[ShapleyAnalyzer] Nhóm tài khoản rỗng, bỏ qua.")
        return {}
    
    accounts_list = list(accounts)
    n = len(accounts_list)
    
    if n == 1:
        val = evaluate_coalition_liquidity({accounts_list[0]}, transactions)
        return {accounts_list[0]: val}
    
    print(f"[ShapleyAnalyzer] Tính toán Shapley Value cho cụm {accounts_list} ({num_permutations} hoán vị)...")
    shapley_values = defaultdict(float)
    
    for _ in range(num_permutations):
        perm = accounts_list.copy()
        random.shuffle(perm)
        
        current_coalition = set()
        current_val = 0.0
        
        for acc in perm:
            current_coalition.add(acc)
            new_val = evaluate_coalition_liquidity(current_coalition, transactions)

            marginal_contribution = new_val - current_val
            shapley_values[acc] += marginal_contribution
            
            current_val = new_val
            
    for acc in accounts_list:
        shapley_values[acc] /= num_permutations
        
    return dict(shapley_values)
