#include "lob.h"
#include <future>
#include <thread>
#include <random>
#include <numeric>
#include <unordered_set>
#include <iostream>

static double normalize_price(double price) 
{
    return std::round(price * 1e8) / 1e8;
}

void LOB::add_order(uint64_t id, Side side, double price, uint64_t volume, uint64_t timestamp)
{
    if (orders.count(id))
    {
        throw std::invalid_argument("Order ID already exists.");
    }
    if (volume == 0)
    {
        throw std::invalid_argument("Volume must be > 0.");
    }

    price = normalize_price(price);
    Order order = {id, price, volume, side, timestamp};

    if (side == Side::BID && !asks.empty()) 
    {
        if (price >= asks.begin()->first) 
        {
            uint64_t filled = 0;
            while (!asks.empty() && filled < volume && price >= asks.begin()->first) 
            {
                uint64_t avail = asks.begin()->second;
                uint64_t to_fill = std::min(volume - filled, avail);
                filled += execute_market_order(Side::BID, to_fill);
            }
            if (filled >= volume) 
            {
                return;
            }
            volume -= filled;
            order = {id, price, volume, side, timestamp};
        }
    } 
    else if (side == Side::ASK && !bids.empty())
    {
        if (price <= bids.begin()->first) 
        {
            uint64_t filled = 0;
            while (!bids.empty() && filled < volume && price <= bids.begin()->first) 
            {
                uint64_t avail = bids.begin()->second;
                uint64_t to_fill = std::min(volume - filled, avail);
                filled += execute_market_order(Side::ASK, to_fill);
            }
            if (filled >= volume) 
            {
                return;
            }
            volume -= filled;
            order = {id, price, volume, side, timestamp};
        }
    }

    orders[id] = order;
    total_orders_added++;

    if (side == Side::BID)
    {
        bids[price] += volume;
        bid_queues[price].push_back(id);
    }
    else
    {
        asks[price] += volume;
        ask_queues[price].push_back(id);
    }
}

void LOB::cancel_order(uint64_t id)
{
    auto it = orders.find(id);
    if (it == orders.end())
    {
        return;
    }

    const Order& order = it->second;
    total_orders_cancelled++;

    if (order.side == Side::BID)
    {
        auto price_it = bids.find(order.price);
        if (price_it != bids.end())
        {

            auto q_it = bid_queues.find(order.price);
            if (q_it != bid_queues.end()) 
            {
                auto& q = q_it->second;
                auto ele_it = std::find(q.begin(), q.end(), id);
                if (ele_it != q.end()) 
                {
                    q.erase(ele_it);
                }
                if (q.empty()) 
                {
                    bid_queues.erase(q_it);
                    bids.erase(price_it);
                } 
                else 
                {
                    if (price_it->second >= order.volume) 
                    {
                        price_it->second -= order.volume;
                    } 
                    else 
                    {
                        uint64_t recalculated_vol = 0;
                        for (uint64_t oid : q_it->second) 
                        {
                            auto ord_it = orders.find(oid);
                            if (ord_it != orders.end()) 
                            {
                                recalculated_vol += ord_it->second.volume;
                            }
                        }
                        price_it->second = recalculated_vol;
                    }
                }
            }
        }
    }
    else
    {
        auto price_it = asks.find(order.price);
        if (price_it != asks.end()) 
        {
            auto q_it = ask_queues.find(order.price);
            if (q_it != ask_queues.end()) 
            {
                auto& q = q_it->second;
                auto ele_it = std::find(q.begin(), q.end(), id);
                if (ele_it != q.end()) 
                {
                    q.erase(ele_it);
                }
                if (q.empty()) 
                {
                    ask_queues.erase(q_it);
                    asks.erase(price_it);
                } 
                else 
                {
                    if (price_it->second >= order.volume) 
                    {
                        price_it->second -= order.volume;
                    } 
                    else 
                    {
                        uint64_t recalculated_vol = 0;
                        for (uint64_t oid : q_it->second) 
                        {
                            auto ord_it = orders.find(oid);
                            if (ord_it != orders.end()) 
                            {
                                recalculated_vol += ord_it->second.volume;
                            }
                        }
                        price_it->second = recalculated_vol;
                    }
                }
            }
        }
    }
    orders.erase(it);
}

uint64_t LOB::execute_market_order(Side side, uint64_t volume)
{
    uint64_t remaining = volume;
    uint64_t total_filled = 0;

    if (side == Side::BID)
    {
        while (!asks.empty() && remaining > 0)
        {
            auto it = asks.begin();
            double price = it->first;

            if (it->second <= remaining)
            {
                remaining -= it->second;
                total_filled += it->second;

                auto q_it = ask_queues.find(price);
                if (q_it != ask_queues.end())
                {
                    for (uint64_t oid : q_it->second)
                    {
                        orders.erase(oid);
                        total_orders_cancelled++;
                    }
                    ask_queues.erase(q_it);
                }
                asks.erase(it);
            }

            else
            {
                total_filled += remaining;
                it->second -= remaining;

                auto q_it = ask_queues.find(price);
                if (q_it != ask_queues.end())
                {
                    uint64_t to_fill = remaining;
                    auto& q = q_it->second;
                    while (to_fill > 0 && !q.empty())
                    {
                        auto ord_it = orders.find(q.front());
                        if (ord_it == orders.end())
                        {
                            q.pop_front();
                            continue;
                        }
                        if (ord_it->second.volume <= to_fill)
                        {
                            to_fill -= ord_it->second.volume;
                            orders.erase(ord_it);
                            total_orders_cancelled++;
                            q.pop_front();
                        }
                        else
                        {
                            ord_it->second.volume -= to_fill;
                            to_fill = 0;
                        }
                    }
                }
                remaining = 0;
            }
        }
    }
    else
    {
        while (!bids.empty() && remaining > 0)
        {
            auto it = bids.begin();
            double price = it->first;

            if (it->second <= remaining)
            {
                remaining -= it->second;
                total_filled += it->second;

                auto q_it = bid_queues.find(price);
                if (q_it != bid_queues.end())
                {
                    for (uint64_t oid : q_it->second)
                    {
                        orders.erase(oid);
                        total_orders_cancelled++;
                    }
                    bid_queues.erase(q_it);
                }
                bids.erase(it);
            }
            else
            {
                total_filled += remaining;
                it->second -= remaining;

                auto q_it = bid_queues.find(price);
                if (q_it != bid_queues.end()) {
                    uint64_t to_fill = remaining;
                    auto& q = q_it->second;
                    while (to_fill > 0 && !q.empty())
                    {
                        auto ord_it = orders.find(q.front());
                        if (ord_it == orders.end())
                        {
                            q.pop_front();
                            continue;
                        }
                        if (ord_it->second.volume <= to_fill)
                        {
                            to_fill -= ord_it->second.volume;
                            orders.erase(ord_it);
                            total_orders_cancelled++;
                            q.pop_front();
                        }
                        else
                        {
                            ord_it->second.volume -= to_fill;
                            to_fill = 0;
                        }
                    }
                }
                remaining = 0;
            }
        }
    }
    return total_filled;
}

double LOB::get_best_bid() const
{
    if (bids.empty())
    {
        return 0;
    }
    return bids.begin()->first;
}

double LOB::get_best_ask() const
{
    if (asks.empty())
    {
        return 0;
    }
    return asks.begin()->first;
}

uint64_t LOB::get_bid_volume(double price) const
{
    auto it = bids.find(normalize_price(price));
    if (it != bids.end())
    {
        return it->second;
    }
    return 0;
}

uint64_t LOB::get_ask_volume(double price) const
{
    auto it = asks.find(normalize_price(price));
    if (it != asks.end())
    {
        return it->second;
    }
    return 0;
}

bool LOB::is_empty() const
{
    return bids.empty() && asks.empty();
}

double LOB::get_spread() const 
{
    if (bids.empty() || asks.empty()) 
    {
        return std::numeric_limits<double>::infinity();
    }
    double best_bid = get_best_bid();
    double best_ask = get_best_ask();
    return best_ask - best_bid;
}

double LOB::get_cancellation_rate() const 
{
    uint64_t added = total_orders_added;
    uint64_t cancelled = total_orders_cancelled;
    if (added == 0) 
    {
        return 0.0;
    }
    return static_cast<double>(cancelled) / static_cast<double>(added);
}

uint64_t LOB::get_total_orders_added() const
{
    return total_orders_added;
}

void LOB::set_total_orders_added(uint64_t val) 
{
    total_orders_added = val;
}

uint64_t LOB::get_total_orders_cancelled() const 
{
    return total_orders_cancelled;
}

void LOB::set_total_orders_cancelled(uint64_t val) 
{
    total_orders_cancelled = val;
}

LOB LOB::clone() const 
{
    return *this;
}

std::vector<Order> LOB::get_all_orders() const 
{
    std::vector<Order> res;
    res.reserve(orders.size());
    std::unordered_set<uint64_t> added_ids;

    for (const auto& kv : bid_queues) 
    {
        for (uint64_t id : kv.second) 
        {
            auto it = orders.find(id);
            if (it != orders.end()) 
            {
                res.push_back(it->second);
                added_ids.insert(id);
            }
        }
    }

    for (const auto& kv : ask_queues) 
    {
        for (uint64_t id : kv.second) 
        {
            auto it = orders.find(id);
            if (it != orders.end()) 
            {
                res.push_back(it->second);
                added_ids.insert(id);
            }
        }
    }

    if (res.size() < orders.size()) 
    {
        for (const auto& kv : orders) 
        {
            if (added_ids.find(kv.first) == added_ids.end()) 
            {
                res.push_back(kv.second);
            }
        }
    }

    return res;
}

void LOB::set_suspect_order_id(uint64_t id) 
{
    suspect_order_id = id;
}

uint64_t LOB::get_suspect_order_id() const 
{
    return suspect_order_id;
}

void LOB::set_market_order_volume(uint64_t vol) 
{
    market_order_volume = vol;
}

uint64_t LOB::get_market_order_volume() const 
{
    return market_order_volume;
}

double LOB::evaluate_state() const 
{
    if (bids.empty() && asks.empty()) 
    {
        return 0;  
    }
    if (bids.empty() || asks.empty()) 
    {
        return 5000;  
    }
    double best_bid = get_best_bid();
    double best_ask = get_best_ask();

    if (best_bid >= best_ask) 
    {
        return 10000;
    }

    double spread = best_ask - best_bid;

    double total_bid_vol = 0;
    double total_ask_vol = 0;
    constexpr int levels = 5;

    int bid_count = 0;
    for (auto it = bids.begin(); it != bids.end() && bid_count < levels; ++it, ++bid_count) 
    {
        total_bid_vol += static_cast<double>(it->second);
    }

    int ask_count = 0;
    for (auto it = asks.begin(); it != asks.end() && ask_count < levels; ++it, ++ask_count) 
    {
        total_ask_vol += static_cast<double>(it->second);
    }

    double imbalance = 0;
    double total_vol = total_bid_vol + total_ask_vol;
    if (total_vol > 0) {
        imbalance = std::abs(total_bid_vol - total_ask_vol)/ total_vol;
    }

    double cancel_penalty = get_cancellation_rate() * 30;

    return spread * 10 + imbalance * 50 + cancel_penalty;
}

bool LOB::is_quiet() const 
{
    double spread = get_spread();
    if (std::isinf(spread)) 
    {
        return true;
    }
    if (spread <= 0) 
    {
        return false;
    }
    return spread >= 2.0;
}

double LOB::q_search(double alpha, double beta, bool is_manipulator, int max_q_depth) 
{
    if (is_manipulator) 
    {
        double stand_pat = evaluate_state();
        if (stand_pat >= beta) 
        {
            return beta;
        }
        if (stand_pat > alpha) 
        {
            alpha = stand_pat;
        }

        if (max_q_depth <= 0) 
        {
            return alpha;
        }
        if (is_quiet()) 
        {
            return alpha;
        }

        if (suspect_order_id != 0 && orders.count(suspect_order_id)) 
        {
            LOB next_state = clone();
            next_state.cancel_order(suspect_order_id);
            double score = next_state.q_search(alpha, beta, false, max_q_depth - 1);
            if (score >= beta) 
            {
                return beta;
            }
            if (score > alpha) 
            {
                alpha = score;
            }
        }
        return alpha;
    }
    else 
    {
        double stand_pat = evaluate_state();
        if (stand_pat <= alpha) 
        {
            return alpha;
        }
        if (stand_pat < beta) 
        {
            beta = stand_pat;
        }

        if (max_q_depth <= 0) 
        {
            return beta;
        }
        if (is_quiet()) 
        {
            return beta;
        }

        double best_bid = get_best_bid();
        if (best_bid > 0) 
        {
            uint64_t best_bid_vol = get_bid_volume(best_bid);
            if (best_bid_vol > 0) 
            {
                LOB next_state = clone();
                next_state.execute_market_order(Side::ASK, best_bid_vol);
                double score = next_state.q_search(alpha, beta, true, max_q_depth - 1);
                if (score <= alpha) 
                {
                    return alpha;
                }
                if (score < beta) 
                {
                    beta = score;
                }
            }
        }
        return beta;
    }
}

double LOB::alpha_beta_search(int depth, double alpha, double beta, bool is_manipulator) 
{
    if (depth == 0 || is_empty()) 
    {
        return q_search(alpha, beta, is_manipulator, 8);
    }

    if (is_manipulator) 
    {
        double max_eval = -1e18;

        LOB move1 = clone();
        double score1 = move1.alpha_beta_search(depth - 1, alpha, beta, false);
        max_eval = std::max(max_eval, score1);
        alpha = std::max(alpha, score1);
        if (beta <= alpha) 
        {
            return max_eval;
        }

        if (suspect_order_id != 0 && orders.count(suspect_order_id)) 
        {
            LOB move2 = clone();
            move2.cancel_order(suspect_order_id);
            double score2 = move2.alpha_beta_search(depth - 1, alpha, beta, false);
            max_eval = std::max(max_eval, score2);
            alpha = std::max(alpha, score2);
        }

        return max_eval;
    } 
    else 
    {
        double min_eval = 1e18;

        LOB move1 = clone();
        double score1 = move1.alpha_beta_search(depth - 1, alpha, beta, true);
        min_eval = std::min(min_eval, score1);
        beta = std::min(beta, score1);
        if (beta <= alpha) 
        {
            return min_eval;
        }

        LOB move2 = clone();
        move2.execute_market_order(Side::ASK, market_order_volume);
        double score2 = move2.alpha_beta_search(depth - 1, alpha, beta, true);
        min_eval = std::min(min_eval, score2);
        beta = std::min(beta, score2);

        return min_eval;
    }
}

std::pair<double, double> LOB::parallel_analyze_scenarios(int num_scenarios) const 
{
    if (num_scenarios <= 0) 
    {
        return {0, 0};
    }

    const unsigned int max_concurrent = std::max(1u, std::thread::hardware_concurrency());
    const int batch_size = static_cast<int>(max_concurrent);

    double total_risk = 0;
    double max_risk = 0;
    int successful_scenarios = 0;

    for (int batch_start = 0; batch_start < num_scenarios; batch_start += batch_size) 
    {
        int batch_end = std::min(batch_start + batch_size, num_scenarios);
        std::vector<std::future<double>> futures;

        for (int i = batch_start; i < batch_end; ++i) 
        {
            futures.push_back(std::async(std::launch::async, [this, i]() 
            {
                LOB s = this->clone();

                thread_local std::mt19937_64 rng(static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()) + i));

                std::uniform_int_distribution<uint64_t> vol_dist(500, 2000);
                s.set_market_order_volume(vol_dist(rng));

                double best_bid = s.get_best_bid();
                double best_ask = s.get_best_ask();

                double bid_min = 0;
                if (best_bid > 0) 
                {
                    bid_min = best_bid * 0.95;
                }
                else 
                {
                    bid_min = 95.0;
                }
                double bid_max = 0;
                if (best_bid > 0) 
                {
                    bid_max = best_bid;
                }
                else 
                {
                    bid_max = 100;
                }
                double ask_min = 0;
                if (best_ask > 0) 
                {
                    ask_min = best_ask;
                }
                else 
                {
                    ask_min = 110;
                }
                double ask_max = 0;
                if (best_ask > 0) 
                {
                    ask_max = best_ask * 1.05;
                }
                else 
                {
                    ask_max = 115.0;
                }   

                std::uniform_int_distribution<int> noise_orders_dist(1, 5);
                std::uniform_int_distribution<int> side_dist(0, 1);
                std::uniform_real_distribution<double> bid_price_dist(bid_min, bid_max);
                std::uniform_real_distribution<double> ask_price_dist(ask_min, ask_max);
                std::uniform_int_distribution<uint64_t> noise_vol_dist(10, 500);

                int num_noise_orders = noise_orders_dist(rng);
                uint64_t noise_id_base = 9000000000ULL + static_cast<uint64_t>(i) * 100;

                for (int j = 0; j < num_noise_orders; ++j) 
                {
                    Side side = side_dist(rng) == 0 ? Side::BID : Side::ASK;
                    double price = (side == Side::BID) ? bid_price_dist(rng) : ask_price_dist(rng);
                    price = normalize_price(std::round(price * 10) / 10);
                    uint64_t volume = noise_vol_dist(rng);

                    if (side == Side::BID && price >= s.get_best_ask() && s.get_best_ask() > 0) 
                    {
                        continue;
                    }
                    if (side == Side::ASK && price <= s.get_best_bid() && s.get_best_bid() > 0) 
                    {
                        continue;
                    }

                    s.add_order(noise_id_base + j, side, price, volume, 2000 + i);
                }

                return s.alpha_beta_search(3, -1e18, 1e18, true);
            }));
        }

        for (auto& fut : futures) 
        {
            try 
            {
                double score = fut.get();
                total_risk += score;
                if (score > max_risk) 
                {
                    max_risk = score;
                }
                successful_scenarios++;
            } 
            catch (const std::exception& e) 
            {
                std::cerr << "[Warning] Scenario execution failed: " << e.what() << std::endl;
            }
        }
    }

    if (successful_scenarios > 0 && successful_scenarios < num_scenarios) 
    {
        total_risk = (total_risk / successful_scenarios) * num_scenarios;
    }

    return {total_risk, max_risk};
}