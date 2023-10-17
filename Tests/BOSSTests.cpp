#define CATCH_CONFIG_RUNNER

#include "../Source/BOSS.hpp"
#include "../Source/BootstrapEngine.hpp"
#include "../Source/ExpressionUtilities.hpp"
#include <arrow/array.h>
#include <arrow/builder.h>
#include <catch2/catch.hpp>
#include <numeric>
#include <variant>

using boss::Expression;
using std::string;
using std::literals::string_literals::operator ""s;
using boss::utilities::operator ""_;
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using boss::expressions::CloneReason;
using boss::expressions::generic::get;
using boss::expressions::generic::get_if;
using boss::expressions::generic::holds_alternative;
namespace boss {
    using boss::expressions::atoms::Span;
};
using std::int64_t;

static std::vector<string>
        librariesToTest{}; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

TEST_CASE("Basics", "[basics]") { // NOLINT
auto engine = boss::engines::BootstrapEngine();
REQUIRE(!librariesToTest.

empty()

);
auto eval = [&engine](boss::Expression &&expression) mutable {
    return engine.evaluate("EvaluateInEngines"_("List"_(GENERATE(from_range(librariesToTest))),
                                                std::move(expression)));
};

//SECTION("Selection") {
//auto const &result =
//        eval("Select"_("Table"_("Column"_("size"_, "List"_(2, 3, 1, 4, 1))),
//                       "Where"_("Greater"_("size"_, 3))));
//REQUIRE(result
//== "List"_("List"_(4,4)));
//}

auto nation = "Table"_(
        "Column"_("n_nationkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("n_regionkey"_, "List"_(1, 1, 2, 3)));

auto part = "Table"_(
        "Column"_("p_partkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("p_retailprice"_, "List"_(100.01, 100.01, 100.01, 100.01)));

auto supplier = "Table"_(
        "Column"_("s_suppkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("s_nationkey"_, "List"_(1, 1, 2, 3)));

auto partsupp = "Table"_(
        "Column"_("ps_partkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("ps_suppkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("ps_supplycost"_, "List"_(771.64, 993.49, 337.09, 357.84)));

auto customer = "Table"_(
        "Column"_("c_custkey"_, "List"_(4, 7, 1, 4)),
        "Column"_("c_nationkey"_, "List"_(15, 13, 1, 4)),
        "Column"_("c_acctbal"_, "List"_(711.56, 121.65, 7498.12, 2866.83)));

auto orders = "Table"_(
        "Column"_("o_orderkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("o_custkey"_, "List"_(4, 7, 1, 4)),
        "Column"_("o_totalprice"_, "List"_(178821.73, 154260.84, 202660.52, 155680.60)),
        "Column"_("o_orderdate"_, "List"_("DateObject"_("1998-01-24"), "DateObject"_("1992-05-01"),
                                          "DateObject"_("1992-12-21"), "DateObject"_("1994-06-18"))),
        "Column"_("o_shippriority"_, "List"_(1, 1, 1, 1)));

auto createSpansInt = [](auto... values) {
    using SpanArguments = boss::expressions::ExpressionSpanArguments;
    std::vector<int32_t> v1 = {values...};
    std::vector<int32_t> v2 = {values...};
    auto s1 = boss::Span<int32_t>(std::move(v1));
    auto s2 = boss::Span<int32_t>(std::move(v2));
    SpanArguments args;
    args.emplace_back(std::move(s1));
    args.emplace_back(std::move(s2));
    return boss::expressions::ComplexExpression("List"_, {}, {}, std::move(args));
};

auto createSpansFloat = [](auto... values) {
    using SpanArguments = boss::expressions::ExpressionSpanArguments;
    std::vector<double_t> v1 = {values...};
    std::vector<double_t> v2 = {values...};
    auto s1 = boss::Span<double_t>(std::move(v1));
    auto s2 = boss::Span<double_t>(std::move(v2));
    SpanArguments args;
    args.emplace_back(std::move(s1));
    args.emplace_back(std::move(s2));
    return boss::expressions::ComplexExpression("List"_, {}, {}, std::move(args));
};

#if 1
auto lineitem = "Table"_(
        "Column"_("l_orderkey"_, createSpansInt(1, 2, 3, 4)),
        "Column"_("l_partkey"_, createSpansInt(1, 2, 3, 4)),
        "Column"_("l_suppkey"_, createSpansInt(1, 2, 3, 4)),
        "Column"_("l_quantity"_, createSpansInt(17, 21, 8, 5)),
        "Column"_("l_extendedprice"_, createSpansFloat(17954.55, 34850.16, 7712.48, 25284.00)),
        "Column"_("l_discount"_, createSpansFloat(0.10, 0.05, 0.06, 0.06)),
        "Column"_("l_tax"_, createSpansFloat(0.02, 0.06, 0.02, 0.06)),
        "Column"_("l_returnflag"_, createSpansInt('N', 'N', 'A', 'A')),
        "Column"_("l_linestatus"_, createSpansInt('O', 'O', 'F', 'F')),
        "Column"_("l_shipdate"_, createSpansInt(1992, 1994,
                                                1996, 1994)));

SECTION("q6") {
auto const &result =
        eval("Group"_(
                "Project"_(
                        "Select"_(
                                "Project"_(std::move(lineitem), "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                                      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                                      "l_extendedprice"_, "l_extendedprice"_)),
                                "Where"_("And"_("Greater"_(24, "l_quantity"_), "Greater"_("l_discount"_, 0.0499),
                                                "Greater"_(0.07001, "l_discount"_),
                                                "Greater"_(1995, "l_shipdate"_),
                                                "Greater"_("l_shipdate"_, 1993)))),
                        "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
                "Sum"_("revenue"_)));
}
#else
auto lineitem = "Table"_(
        "Column"_("l_orderkey"_, "List"_(1, 1, 2, 3)),
        "Column"_("l_partkey"_, "List"_(1, 2, 3, 4)),
        "Column"_("l_suppkey"_, "List"_(1, 2, 3, 4)),
        "Column"_("l_quantity"_, "List"_(17, 21, 8, 5)),
        "Column"_("l_extendedprice"_, "List"_(17954.55, 34850.16, 7712.48, 25284.00)),
        "Column"_("l_discount"_, "List"_(0.10, 0.05, 0.06, 0.06)),
        "Column"_("l_tax"_, "List"_(0.02, 0.06, 0.02, 0.06)),
        "Column"_("l_returnflag"_, "List"_('N', 'N', 'A', 'A')),
        "Column"_("l_linestatus"_, "List"_('O', 'O', 'F', 'F')),
        "Column"_("l_shipdate"_, "List"_("DateObject"_("1992-03-13"), "DateObject"_("1994-04-12"),
                                         "DateObject"_("1996-02-28"), "DateObject"_("1994-12-31"))));

SECTION("q6") {
auto const &result =
        eval("Group"_(
                "Project"_(
                        "Select"_(
                                "Project"_(std::move(lineitem), "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                                      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                                      "l_extendedprice"_, "l_extendedprice"_)),
                                "Where"_("And"_("Greater"_(24, "l_quantity"_), "Greater"_("l_discount"_, 0.0499),
                                                "Greater"_(0.07001, "l_discount"_),
                                                "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                                                "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
                        "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
                "Sum"_("revenue"_)));
}
#endif

auto createGatherTables = [&](auto... values) {
    using ExpressionArguments = boss::expressions::ExpressionArguments;
    using SpanArguments = boss::expressions::ExpressionSpanArguments;
    std::vector<int32_t> v1 = {values...};
    std::vector<int32_t> v2 = {values...};
    auto s1 = boss::Span<int32_t>(std::move(v1));
    auto s2 = boss::Span<int32_t>(std::move(v2));
    SpanArguments positionSpans;
    positionSpans.emplace_back(std::move(s1));
    positionSpans.emplace_back(std::move(s2));
    ExpressionArguments dynamics;
    dynamics.emplace_back(std::move(lineitem));
    return boss::expressions::ComplexExpression("Gather"_, {}, std::move(dynamics), std::move(positionSpans));
};

SECTION("Gather") {
auto const &result = eval("Project"_(std::move(createGatherTables(0, 2, 3)),
                                     "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                           "l_discount"_, "l_returnflag"_, "l_returnflag"_)));
}

SECTION("q1") {
auto const &result = eval("Order"_(
        "Project"_(
                "Group"_(
                        "Project"_(
                                "Project"_(
                                        "Select"_("Project"_(std::move(lineitem),
                                                             "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                                   "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                                   "l_extendedprice"_, "l_extendedprice"_,
                                                                   "l_returnflag"_, "l_returnflag"_,
                                                                   "l_linestatus"_, "l_linestatus"_, "l_tax"_,
                                                                   "l_tax"_)),
                                                  "Where"_(
                                                          "Greater"_(1998, "l_shipdate"_))),
                                        "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                                              "l_quantity"_, "l_quantity"_,
                                              "l_extendedprice"_, "l_extendedprice"_,
                                              "l_discount"_, "l_discount"_,
                                              "calc1"_, "Minus"_(1.0, "l_discount"_),
                                              "calc2"_, "Plus"_(1.0, "l_tax"_))),
                                "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                                      "l_quantity"_, "l_quantity"_, "l_extendedprice"_, "l_extendedprice"_,
                                      "l_discount"_, "l_discount"_,
                                      "disc_price"_, "Times"_("l_extendedprice"_, "calc1"_),
                                      "calc"_, "Times"_("disc_price"_, "calc2"_))),
                        "By"_("l_returnflag"_, "l_linestatus"_),
                        "As"_("sum_qty"_, "Sum"_("l_quantity"_),
                              "sum_base_price"_, "Sum"_("l_extendedprice"_),
                              "sum_disc_price"_, "Sum"_("disc_price"_),
                              "sum_charges"_, "Sum"_("calc"_),
                              "avg_qty"_, "Avg"_("l_quantity"_),
                              "avg_price"_, "Avg"_("l_extendedprice"_),
                              "avg_disc"_, "Avg"_("l_discount"_),
                              "count_order"_, "Count"_("*"_))),
                "By"_("l_returnflag"_, "l_linestatus"_))));
}

SECTION("q3") {
auto const &result =
        eval("Top"_(
                "Group"_(
                        "Project"_(
                                "Join"_(
                                        "Project"_(
                                                "Join"_("Project"_(
                                                                "Select"_("Project"_(std::move(customer),
                                                                                     "As"_("c_custkey"_, "c_custkey"_,
                                                                                           "c_acctbal"_, "c_acctbal"_)),
                                                                          "Where"_("Equal"_("c_acctbal"_, 2866.83))),
                                                                "As"_("c_custkey"_, "c_custkey"_,
                                                                      "c_acctbal"_, "c_acctbal"_)),
                                                        "Select"_(
                                                                "Project"_(std::move(orders),
                                                                           "As"_("o_orderkey"_, "o_orderkey"_,
                                                                                 "o_orderdate"_, "o_orderdate"_,
                                                                                 "o_custkey"_, "o_custkey"_,
                                                                                 "o_shippriority"_, "o_shippriority"_)),
                                                                "Where"_("Greater"_("DateObject"_("1995-03-15"),
                                                                                    "o_orderdate"_))),
                                                        "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                                                "As"_("o_orderkey"_, "o_orderkey"_,
                                                      "o_orderdate"_, "o_orderdate"_,
                                                      "o_custkey"_, "o_custkey"_,
                                                      "o_shippriority"_, "o_shippriority"_)),
                                        "Project"_(
                                                "Select"_(
                                                        "Project"_(std::move(lineitem),
                                                                   "As"_("l_orderkey"_, "l_orderkey"_,
                                                                         "l_discount"_, "l_discount"_,
                                                                         "l_shipdate"_, "l_shipdate"_,
                                                                         "l_extendedprice"_, "l_extendedprice"_)),
                                                        "Where"_("Greater"_("l_shipdate"_,
                                                                            1993))),
                                                "As"_("l_orderkey"_, "l_orderkey"_,
                                                      "l_discount"_, "l_discount"_,
                                                      "l_extendedprice"_, "l_extendedprice"_)),
                                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                                "As"_("Expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                      "l_extendedprice"_, "l_extendedprice"_,
                                      "l_orderkey"_, "l_orderkey"_,
                                      "o_orderdate"_, "o_orderdate"_,
                                      "o_shippriority"_, "o_shippriority"_)),
                        "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                        "As"_("revenue"_, "Sum"_("Expr1009"_))),
                "By"_("revenue"_, "desc"_, "o_orderdate"_), 10));
}

SECTION("q9") {
auto const &result =
        eval("Order"_(
                "Group"_(
                        "Project"_(
                                "Join"_(
                                        "Project"_(
                                                "Join"_(
                                                        "Project"_(
                                                                "Join"_(
                                                                        "Project"_(
                                                                                "Select"_("Project"_(std::move(part),
                                                                                                     "As"_("p_partkey"_,
                                                                                                           "p_partkey"_,
                                                                                                           "p_retailprice"_,
                                                                                                           "p_retailprice"_)),
                                                                                          "Where"_(
                                                                                                  "Equal"_(
                                                                                                          "p_retailprice"_,
                                                                                                          100.01))),
                                                                                "As"_("p_partkey"_,
                                                                                      "p_partkey"_,
                                                                                      "p_retailprice"_,
                                                                                      "p_retailprice"_)),
                                                                        "Project"_(
                                                                                "Join"_(
                                                                                        "Project"_(
                                                                                                "Join"_(
                                                                                                        "Project"_(
                                                                                                                std::move(
                                                                                                                        nation),
                                                                                                                "As"_("n_regionkey"_,
                                                                                                                      "n_regionkey"_,
                                                                                                                      "n_nationkey"_,
                                                                                                                      "n_nationkey"_)),
                                                                                                        "Project"_(
                                                                                                                std::move(
                                                                                                                        supplier),
                                                                                                                "As"_("s_suppkey"_,
                                                                                                                      "s_suppkey"_,
                                                                                                                      "s_nationkey"_,
                                                                                                                      "s_nationkey"_)),
                                                                                                        "Where"_(
                                                                                                                "Equal"_(
                                                                                                                        "n_nationkey"_,
                                                                                                                        "s_nationkey"_))),
                                                                                                "As"_("n_regionkey"_,
                                                                                                      "n_regionkey"_,
                                                                                                      "s_suppkey"_,
                                                                                                      "s_suppkey"_)),
                                                                                        "Project"_(std::move(partsupp),
                                                                                                   "As"_("ps_partkey"_,
                                                                                                         "ps_partkey"_,
                                                                                                         "ps_suppkey"_,
                                                                                                         "ps_suppkey"_,
                                                                                                         "ps_supplycost"_,
                                                                                                         "ps_supplycost"_)),
                                                                                        "Where"_("Equal"_("s_suppkey"_,
                                                                                                          "ps_suppkey"_))),
                                                                                "As"_("n_regionkey"_,
                                                                                      "n_regionkey"_,
                                                                                      "ps_partkey"_,
                                                                                      "ps_partkey"_,
                                                                                      "ps_suppkey"_,
                                                                                      "ps_suppkey"_,
                                                                                      "ps_supplycost"_,
                                                                                      "ps_supplycost"_)),
                                                                        "Where"_("Equal"_(
                                                                                "p_partkey"_, "ps_partkey"_))),
                                                                "As"_("n_regionkey"_, "n_regionkey"_,
                                                                      "ps_partkey"_, "ps_partkey"_,
                                                                      "ps_suppkey"_, "ps_suppkey"_,
                                                                      "ps_supplycost"_,
                                                                      "ps_supplycost"_)),
                                                        "Project"_(std::move(lineitem),
                                                                   "As"_("l_partkey"_, "l_partkey"_,
                                                                         "l_suppkey"_, "l_suppkey"_,
                                                                         "l_orderkey"_, "l_orderkey"_,
                                                                         "l_extendedprice"_,
                                                                         "l_extendedprice"_,
                                                                         "l_discount"_, "l_discount"_,
                                                                         "l_quantity"_, "l_quantity"_)),
                                                        "Where"_("Equal"_(
                                                                "List"_("ps_partkey"_, "ps_suppkey"_),
                                                                "List"_("l_partkey"_, "l_suppkey"_)))),
                                                "As"_("n_regionkey"_, "n_regionkey"_,
                                                      "ps_supplycost"_, "ps_supplycost"_,
                                                      "l_orderkey"_, "l_orderkey"_,
                                                      "l_extendedprice"_, "l_extendedprice"_,
                                                      "l_discount"_, "l_discount"_,
                                                      "l_quantity"_, "l_quantity"_)),
                                        "Project"_(std::move(orders),
                                                   "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                         "o_orderdate"_)),
                                        "Where"_("Equal"_(
                                                "o_orderkey"_, "l_orderkey"_))),
                                "As"_("nation"_, "n_regionkey"_,
                                      "o_year"_, "Year"_("o_orderdate"_),
                                      "amount"_, "Minus"_("Times"_(
                                                                  "l_extendedprice"_,
                                                                  "Minus"_(1.0, "l_discount"_)),
                                                          "Times"_("ps_supplycost"_,
                                                                   "l_quantity"_)))),
                        "By"_("nation"_, "o_year"_),
                        "Sum"_("amount"_)),
                "By"_("nation"_, "o_year"_, "desc"_)));
}

SECTION("q18") {
auto const &result =
        eval("Top"_(
                "Group"_(
                        "Project"_(
                                "Join"_(
                                        "Select"_("Group"_("Project"_(
                                                                   std::move(lineitem),
                                                                   "As"_("l_orderkey"_, "l_orderkey"_,
                                                                         "l_quantity"_, "l_quantity"_)),
                                                           "By"_("l_orderkey"_),
                                                           "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                                                  "Where"_("Greater"_("sum_l_quantity"_, 1))),
                                        "Project"_(
                                                "Join"_("Project"_(std::move(customer),
                                                                   "As"_("c_acctbal"_, "c_acctbal"_,
                                                                         "c_custkey"_,
                                                                         "c_custkey"_)),
                                                        "Project"_(std::move(orders),
                                                                   "As"_("o_orderkey"_,
                                                                         "o_orderkey"_,
                                                                         "o_custkey"_,
                                                                         "o_custkey"_,
                                                                         "o_orderdate"_,
                                                                         "o_orderdate"_,
                                                                         "o_totalprice"_,
                                                                         "o_totalprice"_)),
                                                        "Where"_("Equal"_("c_custkey"_,
                                                                          "o_custkey"_))),
                                                "As"_("c_acctbal"_, "c_acctbal"_,
                                                      "o_orderkey"_, "o_orderkey"_,
                                                      "o_custkey"_, "o_custkey"_,
                                                      "o_orderdate"_, "o_orderdate"_,
                                                      "o_totalprice"_, "o_totalprice"_)),
                                        "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))),
                                "As"_("o_orderkey"_, "o_orderkey"_,
                                      "o_orderdate"_, "o_orderdate"_,
                                      "o_totalprice"_, "o_totalprice"_,
                                      "c_acctbal"_, "c_acctbal"_,
                                      "o_custkey"_, "o_custkey"_,
                                      "sum_l_quantity"_, "sum_l_quantity"_)),
                        "By"_("c_acctbal"_, "o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_totalprice"_),
                        "Sum"_("sum_l_quantity"_)),
                "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100));
}
}

int main(int argc, char *argv[]) {
    Catch::Session session;
    session.cli(session.cli() | Catch::clara::Opt(librariesToTest, "library")["--library"]);
    int returnCode = session.applyCommandLine(argc, argv);
    if (returnCode != 0) {
        return returnCode;
    }
    return session.run();
}