#pragma once

#include <map>
#include <unordered_map>
#include <deque>
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <limits>
#include <algorithm>
#include <cmath>
#include <atomic>
#include <functional>

enum class Side 
{
    BID,
    ASK
};

struct Order 
{
    uint64_t id;
    double price;
    uint64_t volume;
    Side side;
    uint64_t timestamp;
};


class LOB 
{
private:

    std::map<double, uint64_t, std::greater<double>> bids; 
    std::map<double, uint64_t, std::less<double>> asks;    

    std::unordered_map<uint64_t, Order> orders;

    std::map<double, std::deque<uint64_t>, std::greater<double>> bid_queues;
    std::map<double, std::deque<uint64_t>, std::less<double>> ask_queues;

    uint64_t suspect_order_id = 0;
    uint64_t market_order_volume = 1000;

    uint64_t total_orders_added{0};
    uint64_t total_orders_cancelled{0};

public:
    LOB() = default;

    LOB(const LOB& other) 
    {
        bids = other.bids;
        asks = other.asks;
        orders = other.orders;
        bid_queues = other.bid_queues;
        ask_queues = other.ask_queues;
        suspect_order_id = other.suspect_order_id;
        market_order_volume = other.market_order_volume;
        total_orders_added = other.total_orders_added;
        total_orders_cancelled = other.total_orders_cancelled;
    }

    LOB& operator=(const LOB& other) 
    {
        if (this != &other) 
        {
            bids = other.bids;
            asks = other.asks;
            orders = other.orders;
            bid_queues = other.bid_queues;
            ask_queues = other.ask_queues;
            suspect_order_id = other.suspect_order_id;
            market_order_volume = other.market_order_volume;
            total_orders_added = other.total_orders_added;
            total_orders_cancelled = other.total_orders_cancelled;
        }
        return *this;
    }

    void set_suspect_order_id(uint64_t id);
    uint64_t get_suspect_order_id() const;
    void set_market_order_volume(uint64_t vol);
    uint64_t get_market_order_volume() const;

    uint64_t get_total_orders_added() const;
    void set_total_orders_added(uint64_t val);
    uint64_t get_total_orders_cancelled() const;
    void set_total_orders_cancelled(uint64_t val);

    std::vector<Order> get_all_orders() const;

    void add_order(uint64_t id, Side side, double price, uint64_t volume, uint64_t timestamp);

    void cancel_order(uint64_t id);

    uint64_t execute_market_order(Side side, uint64_t volume);

    double get_best_bid() const;
    double get_best_ask() const;
    uint64_t get_bid_volume(double price) const;
    uint64_t get_ask_volume(double price) const;
    bool is_empty() const;

    double get_spread() const;

    double get_cancellation_rate() const;

    LOB clone() const;

    double evaluate_state() const;

    bool is_quiet() const;

    double q_search(double alpha, double beta, bool is_manipulator, int max_q_depth = 8);

    double alpha_beta_search(int depth, double alpha, double beta, bool is_manipulator);

    std::pair<double, double> parallel_analyze_scenarios(int num_scenarios) const;
};
