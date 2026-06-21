#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "lob.h"

namespace py = pybind11;

PYBIND11_MODULE(lob_core, m) 
{
    m.doc() = "C++ Limit Order Book core optimized with Red-Black Trees";

    py::enum_<Side>(m, "Side")
        .value("BID", Side::BID)
        .value("ASK", Side::ASK)
        .export_values();

    py::class_<Order>(m, "Order")
        .def(py::init<>())
        .def_readwrite("id", &Order::id)
        .def_readwrite("price", &Order::price)
        .def_readwrite("volume", &Order::volume)
        .def_readwrite("side", &Order::side)
        .def_readwrite("timestamp", &Order::timestamp)
        .def(py::pickle(
            [](const Order &o) {
                return py::make_tuple(o.id, o.price, o.volume, o.side, o.timestamp);
            },
            [](py::tuple t) {
                if (t.size() != 5) throw std::runtime_error("Invalid state!");
                Order o;
                o.id = t[0].cast<uint64_t>();
                o.price = t[1].cast<double>();
                o.volume = t[2].cast<uint64_t>();
                o.side = t[3].cast<Side>();
                o.timestamp = t[4].cast<uint64_t>();
                return o;
            }
        ));

    py::class_<LOB>(m, "LOB")
        .def(py::init<>())
        .def("add_order", &LOB::add_order, "Thêm lệnh Limit vào sổ", 
             py::arg("id"), py::arg("side"), py::arg("price"), py::arg("volume"), py::arg("timestamp"))
        .def("cancel_order", &LOB::cancel_order, "Hủy lệnh", py::arg("id"))
        .def("execute_market_order", &LOB::execute_market_order, "Khớp lệnh Market (trả về volume đã khớp)", 
             py::arg("side"), py::arg("volume"))
        .def("get_best_bid", &LOB::get_best_bid)
        .def("get_best_ask", &LOB::get_best_ask)
        .def("get_bid_volume", &LOB::get_bid_volume, py::arg("price"))
        .def("get_ask_volume", &LOB::get_ask_volume, py::arg("price"))
        .def("get_spread", &LOB::get_spread)
        .def("is_empty", &LOB::is_empty)
        .def("get_cancellation_rate", &LOB::get_cancellation_rate)
        .def("get_total_orders_added", &LOB::get_total_orders_added)
        .def("set_total_orders_added", &LOB::set_total_orders_added, py::arg("val"))
        .def("get_total_orders_cancelled", &LOB::get_total_orders_cancelled)
        .def("set_total_orders_cancelled", &LOB::set_total_orders_cancelled, py::arg("val"))
        .def("get_all_orders", &LOB::get_all_orders)
        .def("clone", &LOB::clone, "Clone sổ lệnh sang một đối tượng độc lập")
        .def("alpha_beta_search", &LOB::alpha_beta_search, "Chạy Alpha-Beta Search trực tiếp bằng C++", 
             py::arg("depth"), py::arg("alpha"), py::arg("beta"), py::arg("is_manipulator"))
        .def("q_search", &LOB::q_search, "Chạy Quiescence Search trực tiếp bằng C++", 
             py::arg("alpha"), py::arg("beta"), py::arg("is_manipulator"), py::arg("max_q_depth") = 8)
        .def("evaluate_state", &LOB::evaluate_state, "Hàm heuristic đánh giá bằng C++")
        .def("set_suspect_order_id", &LOB::set_suspect_order_id, py::arg("id"))
        .def("get_suspect_order_id", &LOB::get_suspect_order_id)
        .def("set_market_order_volume", &LOB::set_market_order_volume, py::arg("vol"))
        .def("get_market_order_volume", &LOB::get_market_order_volume)
        .def("parallel_analyze_scenarios", &LOB::parallel_analyze_scenarios, "Chạy song song các kịch bản trong C++", py::arg("num_scenarios"))
        .def(py::pickle(
            [](const LOB &l) 
            { 
                return py::make_tuple(
                    l.get_all_orders(), 
                    l.get_suspect_order_id(), 
                    l.get_market_order_volume(),
                    l.get_total_orders_added(),
                    l.get_total_orders_cancelled()
                );
            },
            [](py::tuple t) 
            { 
                if (t.size() != 5) 
                {
                    throw std::runtime_error("Invalid state tuple size!");
                }
                LOB l;
                auto orders = t[0].cast<std::vector<Order>>();
                for (const auto& o : orders) {
                    l.add_order(o.id, o.side, o.price, o.volume, o.timestamp);
                }
                l.set_suspect_order_id(t[1].cast<uint64_t>());
                l.set_market_order_volume(t[2].cast<uint64_t>());
                l.set_total_orders_added(t[3].cast<uint64_t>());
                l.set_total_orders_cancelled(t[4].cast<uint64_t>());
                return l;
            }
        ));
}
