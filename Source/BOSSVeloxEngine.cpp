#include "BOSSVeloxEngine.hpp"

#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#include <Utilities.hpp>
#include <any>
#include <iterator>
#include <list>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <ExpressionUtilities.hpp>
#include <cstring>
#include <iostream>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

using std::endl;
using std::to_string;
using std::function;
using std::get;
using std::is_invocable_v;
using std::move;
using std::unordered_map;
using std::string_literals::operator ""s;
using boss::utilities::operator ""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;
using boss::Expression;
using boss::expressions::generic::isComplexExpression;


std::ostream &operator<<(std::ostream &s, std::vector<std::int64_t> const &input /*unused*/) {
    std::for_each(begin(input), prev(end(input)),
                  [&s = s << "["](auto &&element) { s << element << ", "; });
    return (input.empty() ? s : (s << input.back())) << "]";
}

namespace boss {
    using std::vector;
//    using SpanInputs = std::variant<vector<bool>, vector<std::int64_t>, vector<std::double_t>,
//            vector<std::string>, vector<Symbol>>;
    using SpanInputs = std::variant</*vector<bool>, */vector<std::int64_t>, vector<std::double_t>,
            vector<std::string>, vector<Symbol>>;
} // namespace boss

namespace boss::engines::velox {
    using VeloxExpressionSystem = ExtensibleExpressionSystem<>;
    using AtomicExpression = VeloxExpressionSystem::AtomicExpression;
    using ComplexExpression = VeloxExpressionSystem::ComplexExpression;
    template<typename... T>
    using ComplexExpressionWithStaticArguments =
            VeloxExpressionSystem::ComplexExpressionWithStaticArguments<T...>;
    using Expression = VeloxExpressionSystem::Expression;
    using ExpressionArguments = VeloxExpressionSystem::ExpressionArguments;
    using ExpressionSpanArguments = VeloxExpressionSystem::ExpressionSpanArguments;
    using ExpressionSpanArgument = VeloxExpressionSystem::ExpressionSpanArgument;
    using expressions::generic::ArgumentWrapper;
    using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;

// reference counter class to track references for Span and af::array pointers to memory
// calling a destructor once the reference count reaches 0
    class SpanReferenceCounter {
    public:
        // call destructor only for the initial caller of add(), who is the owner of the data
        void add(void *data, std::function<void(void)> &&destructor = {}) {
            auto &info = map.try_emplace(data, std::move(destructor)).first->second;
            info.counter++;
        }

        void remove(void *data) {
            auto it = map.find(data);
            if (--it->second.counter == 0) {
                if (it->second.destructor) {
                    it->second.destructor();
                }
                map.erase(it);
            }
        }

    private:
        struct Info {
            std::function<void(void)> destructor;
            unsigned int counter = 0;

            explicit Info(std::function<void(void)> &&f) : destructor(std::move(f)) {}
        };

        unordered_map<void *, Info> map;
    };

    static SpanReferenceCounter spanReferenceCounter;

    int64_t dateToInt64(std::string str) {
        std::istringstream iss;
        iss.str(str);
        struct std::tm tm = {};
        iss >> std::get_time(&tm, "%Y-%m-%d");
        int64_t t = std::mktime(&tm);
        return (int64_t) std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::from_time_t(t).time_since_epoch())
                .count();
    }

    Expression dateProcess(Expression &&e) {
        return visit(boss::utilities::overload(
                             [](ComplexExpression &&e) -> Expression {
                                 auto head = e.getHead();
                                 if (head.getName() == "DateObject") {
                                     auto argument = e.getArguments().at(0);
                                     std::stringstream out;
                                     out << argument;
                                     std::string dateString = out.str().substr(1, 10);
                                     return dateToInt64(dateString);
                                 }
                                 // at least evaluate all the arguments
                                 auto [_, statics, dynamics, spans] = std::move(e).decompose();
                                 std::transform(std::make_move_iterator(dynamics.begin()),
                                                std::make_move_iterator(dynamics.end()),
                                                dynamics.begin(),
                                                [](auto &&arg) { return dateProcess(std::move(arg)); });
                                 return ComplexExpression{std::move(head), std::move(statics),
                                                          std::move(dynamics), std::move(spans)};
                             },
                             [](auto &&e) -> Expression { return std::forward<decltype(e)>(e); }),
                     std::move(e));
    }

    template<typename... StaticArgumentTypes>
    ComplexExpressionWithStaticArguments<StaticArgumentTypes...>
    transformDynamicsToSpans(ComplexExpressionWithStaticArguments<StaticArgumentTypes...> &&input_) {
        std::vector<boss::SpanInputs> spanInputs;
        auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();

        auto it = std::move_iterator(dynamics.begin());
        for (; it != std::move_iterator(dynamics.end()); ++it) {
            if (!std::visit(
                    [&spanInputs]<typename InputType>(InputType &&argument) {
                        using Type = std::decay_t<InputType>;
                        if constexpr (boss::utilities::isVariantMember<std::vector<Type>,
                                boss::SpanInputs>::value) {
                            if (!spanInputs.empty() &&
                                std::holds_alternative<std::vector<Type>>(spanInputs.back())) {
                                std::get<std::vector<Type>>(spanInputs.back()).push_back(argument);
                            } else {
                                spanInputs.push_back(std::vector<Type>{argument});
                            }
                            return true;
                        }
                        return false;
                    },
                    dateProcess(*it))) {
                break;
            }
        }
        dynamics.erase(dynamics.begin(), it.base());

        ExpressionSpanArguments spans;
        std::transform(
                std::move_iterator(spanInputs.begin()), std::move_iterator(spanInputs.end()),
                std::back_inserter(spans), [](auto &&untypedInput) {
                    return std::visit(
                            []<typename Element>(std::vector<Element> &&input) -> ExpressionSpanArgument {
//                                return boss::Span<Element>(std::move(input));
                                auto *ptr = input.data();
                                auto size = input.size();
                                spanReferenceCounter.add(ptr, [v = std::move(input)]() {});
                                return boss::Span<Element>(ptr, size,
                                                           [ptr](auto &&) { spanReferenceCounter.remove(ptr); });
                            },
                            std::forward<decltype(untypedInput)>(untypedInput));
                });

        std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
                  std::back_inserter(spans));
        return {std::move(head), std::move(statics), std::move(dynamics), std::move(spans)};
    }

    Expression transformDynamicsToSpans(Expression &&input) {
        return std::visit(
                [](auto &&x) -> Expression {
                    if constexpr (std::is_same_v<std::decay_t<decltype(x)>, ComplexExpression>) {
                        return transformDynamicsToSpans(std::forward<decltype(x)>(x));
                    } else {
                        return x;
                    }
                },
                std::move(input));
    }

    template<typename VisiteeT, typename... VisitorTs>
    decltype(auto) visitArgument(VisiteeT &&visitee, VisitorTs... visitors) {
        return std::visit(boss::utilities::overload(
                                  std::forward<decltype(visitors)>(visitors)...,
                                  [](auto &&e) -> Expression { return std::forward<decltype(e)>(e); }),
                          std::forward<decltype(visitee)>(visitee));
    }

    template<typename T>
    VectorPtr spanToVelox(boss::Span<T> const &span, memory::MemoryPool *pool) {
        auto bossArray = makeBossArray(span.begin(), span.size());
        if constexpr (std::is_same_v<T, int64_t>) {
            return importFromBossAsViewer(BossType::bBIGINT, bossArray, pool);
        } else if constexpr (std::is_same_v<T, double_t>) {
            return importFromBossAsViewer(BossType::bDOUBLE, bossArray, pool);
        }
    }

    ExpressionSpanArgument veloxtoSpan(const VectorPtr &vec) {
        if ((vec->typeKind() == TypeKind::BIGINT) ||
            (vec->typeKind() == TypeKind::TINYINT) ||
            (vec->typeKind() == TypeKind::SMALLINT) ||
            (vec->typeKind() == TypeKind::INTEGER)) {
            return boss::Span<const int64_t>(vec->values()->as<int64_t>(), vec->size(),
                                             [](auto && /*unused*/) {});
        } else if ((vec->typeKind() == TypeKind::DOUBLE) ||
                   (vec->typeKind() == TypeKind::REAL)) {
            return boss::Span<const double_t>(vec->values()->as<double_t>(), vec->size(),
                                              [](auto && /*unused*/) {});
        } else {
            throw std::runtime_error("veloxToSpan: array type not supported");
        }
    }

    bool cmpFunCheck(std::string input) {
        static std::vector<std::string> cmpFun{"Greater", "Equal", "StringContains"};
        for (int i = 0; i < cmpFun.size(); i++) {
            if (input == cmpFun[i]) {
                return 1;
            }
        }
        return 0;
    }

    void formatVeloxProjection(std::vector<std::string> &projectionList,
                               std::unordered_map<std::string, std::string> projNameMap) {
        std::vector<std::string> input(3);
        std::string out;
        for (int i = 0; i < 3; i++) {
            auto tmp = projectionList.back();
            auto it = projNameMap.find(tmp);
            if (it != projNameMap.end())
                tmp = it->second;
            input[2 - i] = tmp;
            projectionList.pop_back();
        }

        if (input[0] == "Multiply" || input[0] == "Times") {
            out = fmt::format("({}) * ({})", input[1], input[2]);
        } else if (input[0] == "Plus") {
            out = fmt::format("({}) + ({})", input[1], input[2]);
        } else if (input[0] == "Minus" || input[0] == "Subtract") {
            out = fmt::format("({}) - ({})", input[1], input[2]);
        } else VELOX_FAIL("unexpected Projection type");
        projectionList.push_back(out);
    }

    void bossExprToVeloxFilter_Join(Expression const &expression, QueryBuilder &queryBuilder) {
        static FiledFilter tmpFieldFilter;
        static JoinPairList tmpJoinPairList;
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            AtomicExpr element;
                            element.data = to_string(a);
                            element.type = cValue;
                            tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](std::vector<bool>::reference a) {
                        },
                        [&](char const *a) {
                            AtomicExpr element;
                            element.data = a;
                            element.type = cValue;
                            tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](Symbol const &a) {
                            if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                          queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                          a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                            AtomicExpr element;
                            element.data = a.getName();
                            element.type = cName;
                            tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](std::string const &a) {
                            AtomicExpr element;
                            element.data = "'" + a + "'";
                            element.type = cValue;
                            tmpFieldFilter.element.emplace_back(element);
                        },
                        [&](ComplexExpression const &expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (cmpFunCheck(headName)) { // field filter or join op pre-process
                                tmpFieldFilter.clear();
                                tmpFieldFilter.opName = headName;
                            }
                            auto process = [&](auto const &arguments) {
                                for (auto it = arguments.begin(); it != arguments.end(); ++it) {
                                    auto const &argument = *it;
#ifdef DebugInfo
                                    std::cout << "argument  " << argument << endl;
#endif
                                    std::stringstream out;
                                    out << argument;
                                    std::string tmpArhStr = out.str();

                                    if (tmpArhStr.substr(0, 10) == "DateObject") {
                                        std::string dateString = tmpArhStr.substr(12, 10);

                                        auto dateInt64 = dateToInt64(dateString);

                                        AtomicExpr element;
                                        element.data = to_string(dateInt64);
                                        element.type = cValue;
                                        tmpFieldFilter.element.emplace_back(element);
                                        continue;
                                    }
                                    if (tmpArhStr.substr(0, 4) == "Date") {
                                        std::string dateString = tmpArhStr.substr(6, 10);
                                        auto dateInt64 = dateToInt64(dateString);

                                        AtomicExpr element;
                                        element.data = to_string(dateInt64);
                                        element.type = cValue;
                                        tmpFieldFilter.element.emplace_back(element);
                                        continue;
                                    }
                                    if (headName == "List") {
                                        if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                      queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                                      tmpArhStr) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                            queryBuilder.curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                        if (!tmpJoinPairList.leftFlag)
                                            tmpJoinPairList.leftKeys.emplace_back(tmpArhStr);
                                        else
                                            tmpJoinPairList.rightKeys.emplace_back(tmpArhStr);
                                        continue;
                                    }

                                    std::visit([&queryBuilder](auto &&argument) {
                                                   return bossExprToVeloxFilter_Join(argument, queryBuilder);
                                               },
                                               argument.getArgument());
                                }
                                if (!tmpFieldFilter.opName.empty()) { // field filter or join op post-process
                                    if (headName == "Greater")
                                        queryBuilder.mergeGreaterFilter(tmpFieldFilter);
                                    else if ((headName == "Equal" || headName == "StringContains") &&
                                             tmpFieldFilter.element.size() > 0) {
                                        queryBuilder.curVeloxExpr.tmpFieldFiltersVec.push_back(tmpFieldFilter);
                                    }
                                }
                                if (headName == "List") {
                                    if (!tmpJoinPairList.leftFlag)
                                        tmpJoinPairList.leftFlag = true;
                                    else {
                                        queryBuilder.curVeloxExpr.hashJoinListVec.push_back(tmpJoinPairList);
                                        tmpJoinPairList.clear();
                                    }
                                }
                            };
                            process(expression.getArguments());
                        }),
                (Expression::SuperType const &) expression);
    }

    void bossExprToVeloxProj_PartialAggr(Expression const &expression, std::vector<std::string> &projectionList,
                                         QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            projectionList.push_back(to_string(a));
                        },
                        [&](std::vector<bool>::reference a) {
                        },
                        [&](char const *a) {
                            projectionList.push_back(a);
                        },
                        [&](Symbol const &a) {
                            if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                          queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                          a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                            projectionList.push_back(a.getName());
                        },
                        [&](std::string const &a) {
                            projectionList.push_back(a);
                        },
                        [&](ComplexExpression const &expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            projectionList.push_back(headName);
                            auto process = [&](auto const &arguments) {
                                for (auto it = arguments.begin(); it != arguments.end(); ++it) {
                                    auto const &argument = *it;
#ifdef DebugInfo
                                    std::cout << "argument  " << argument << endl;
#endif
                                    std::stringstream out;
                                    out << argument;
                                    std::string tmpArhStr = out.str();

                                    if (headName == "Sum" || headName == "Avg" || headName == "Count") {
                                        aggrPair aggregation;
                                        auto str = headName;
                                        std::transform(str.begin(), str.end(), str.begin(), ::tolower);
                                        aggregation.op = str;
                                        aggregation.oldName = tmpArhStr;
                                        aggregation.newName = "";
                                        queryBuilder.curVeloxExpr.aggregatesVec.push_back(aggregation);
                                        if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                      queryBuilder.curVeloxExpr.selectedColumns.end(),
                                                      tmpArhStr) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                            queryBuilder.curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                        continue;
                                    }

                                    std::visit(
                                            [&projectionList, &queryBuilder](auto &&argument) {
                                                return bossExprToVeloxProj_PartialAggr(argument, projectionList,
                                                                                       queryBuilder);
                                            },
                                            argument.getArgument());
                                    if (headName == "Year") {
//                              auto out = fmt::format("year({})", projectionList.back());
                                        // Date type has already been treated as int64
                                        auto out = projectionList.back();
                                        projectionList.pop_back();
                                        projectionList.pop_back();
                                        projectionList.push_back(out);
                                    }
                                }
                                if (projectionList.size() >= 3) { // field filter or join op post-process
                                    formatVeloxProjection(projectionList, queryBuilder.projNameMap);
                                }
                            };
                            process(expression.getArguments());
                        }),
                (Expression::SuperType const &) expression);
    }

    void bossExprToVeloxBy(Expression const &expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](char const *a) {
                        },
                        [&](Symbol const &a) {
                            if (queryBuilder.curVeloxExpr.orderBy) {
                                if (!strcasecmp(a.getName().c_str(), "desc")) {
                                    auto it = queryBuilder.curVeloxExpr.orderByVec.end() - 1;
                                    *it = *it + " DESC";
                                } else {
                                    auto it = queryBuilder.aggrNameMap.find(a.getName());
                                    if (it != queryBuilder.aggrNameMap.end())
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(it->second);
                                    else
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(a.getName());
                                }
                            } else {
                                if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                              queryBuilder.curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                              a.getName()) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                    queryBuilder.curVeloxExpr.selectedColumns.push_back(a.getName());
                                queryBuilder.curVeloxExpr.groupingKeysVec.push_back(a.getName());
                            }
                        },
                        [&](std::string const &a) {
                        },
                        [&](ComplexExpression const &expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            auto process = [&](auto const &arguments) {
                                for (auto it = arguments.begin(); it != arguments.end(); ++it) {
                                    auto const &argument = *it;
#ifdef DebugInfo
                                    std::cout << "argument  " << argument << endl;
#endif
                                    std::stringstream out;
                                    out << argument;
                                    std::string tmpArhStr = out.str();
                                    std::visit(
                                            [&queryBuilder](auto &&argument) {
                                                return bossExprToVeloxBy(argument, queryBuilder);
                                            },
                                            argument.getArgument());
                                }
                            };
                            process(expression.getArguments());
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                (Expression::SuperType const &) expression);
    }

// "Table"_("Column"_("Id"_, "List"_(1, 2, 3), "Column"_("Value"_ "List"_(0.1, 10.0, 5.2)))
    std::unordered_map<std::string, TypePtr> getColumns(
            ComplexExpression const &expression, RowVectorPtr &rowData, memory::MemoryPool *pool) {
        ExpressionArguments columns = std::move(expression).getArguments();
        std::vector<std::string> colNameVec;
        std::vector<VectorPtr> colDataVec;
        std::unordered_map<std::string, TypePtr> fileColumnNamesMap(columns.size());

        std::transform(
                std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
                std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
                [&colNameVec, &colDataVec, pool](auto &&columnExpr) {
                    auto column = get < ComplexExpression > (std::forward<decltype(columnExpr)>(columnExpr));
                    auto [head, unused_, dynamics, spans] = std::move(column).decompose();
                    auto columnName = get < Symbol > (std::move(dynamics.at(0)));
                    auto dynamic = get < ComplexExpression > (std::move(dynamics.at(1)));
                    auto list = transformDynamicsToSpans(std::move(dynamic));
                    auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();

                    auto colData = std::visit(
                            [pool]<typename T>(boss::Span<T> &&typedSpan) -> VectorPtr {
                                if constexpr (std::is_same_v<T, int64_t> || std::is_same_v<T, double_t>) {
                                    return spanToVelox<T>(typedSpan, pool);
                                } else {
                                    throw std::runtime_error("unsupported column type in Select");
                                }
                            },
                            std::move(listSpans.at(0)));

                    auto columnType = colData->type();
                    colNameVec.emplace_back(columnName.getName());
                    colDataVec.push_back(std::move(colData));
                    return std::make_pair(columnName.getName(), columnType);
                });
        rowData = makeRowVector(colNameVec, colDataVec, pool);

        return fileColumnNamesMap;
    }

    void bossExprToVelox(Expression const &expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](bool a) {
                        },
                        [&](std::vector<bool>::reference a) {
                        },
                        [&](std::int64_t a) {
                            queryBuilder.curVeloxExpr.limit = a;
                        },
                        [&](char const *a) {
                        },
                        [&](std::double_t a) {
                        },
                        [&](Symbol const &a) {
                        },
                        [&](std::string const &a) {
                        },
                        [&](ComplexExpression const &expression) {
                            std::string projectionName;
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (headName == "Table") {
                                if (!queryBuilder.curVeloxExpr.tableName.empty()) // traverse for a new plan builder, triggered by Table head
                                {
                                    queryBuilder.veloxExprList.push_back(queryBuilder.curVeloxExpr);
                                    queryBuilder.curVeloxExpr.clear();
                                }
                                queryBuilder.curVeloxExpr.tableName = fmt::format("Table{}",
                                                                                  queryBuilder.tableCnt++); // indicate table names
                                queryBuilder.curVeloxExpr.fileColumnNamesMap = getColumns(expression,
                                                                                          queryBuilder.curVeloxExpr.rowData,
                                                                                          queryBuilder.pool_.get());
                                return;
                            }
                            auto process = [&](auto const &arguments) {
                                std::vector<std::string> lastProjectionsVec;
                                if (headName ==
                                    "As") { //save the latest ProjectionsVec to restore for AS - aggregation
                                    lastProjectionsVec = queryBuilder.curVeloxExpr.projectionsVec;
                                    queryBuilder.curVeloxExpr.projectionsVec.clear();
                                }
                                for (auto it = arguments.begin(); it != arguments.end(); ++it) {
                                    auto const &argument = *it;
#ifdef DebugInfo
                                    std::cout << "argument  " << argument << endl;
#endif
                                    std::stringstream out;
                                    out << argument;
                                    std::string tmpArhStr = out.str();

                                    if (headName == "Where") {
                                        std::visit(
                                                [&queryBuilder](auto &&argument) {
                                                    return bossExprToVeloxFilter_Join(argument, queryBuilder);
                                                },
                                                argument.getArgument());
                                        if (queryBuilder.curVeloxExpr.tmpFieldFiltersVec.size() > 0)
                                            queryBuilder.formatVeloxFilter_Join();
                                    } else if (headName == "As") {
                                        if ((it - arguments.begin()) % 2 == 0) {
                                            projectionName = tmpArhStr;
                                        } else {
                                            std::vector<std::string> projectionList;
                                            std::visit(
                                                    [&projectionList, &queryBuilder](auto &&argument) {
                                                        return bossExprToVeloxProj_PartialAggr(argument,
                                                                                               projectionList,
                                                                                               queryBuilder);
                                                    },
                                                    argument.getArgument());

                                            // fill the new name for aggregation
                                            if (!queryBuilder.curVeloxExpr.aggregatesVec.empty()) {
                                                auto &aggregation = queryBuilder.curVeloxExpr.aggregatesVec.back();
                                                if (aggregation.newName == "") {
                                                    aggregation.newName = projectionName;
                                                    queryBuilder.curVeloxExpr.orderBy = true;
                                                }
                                                queryBuilder.curVeloxExpr.projectionsVec = lastProjectionsVec;
                                            } else {
                                                std::string projection;
                                                if (projectionList[0] != projectionName)
                                                    projection = fmt::format("{} AS {}", projectionList[0],
                                                                             projectionName);
                                                else
                                                    projection = projectionList[0];
                                                queryBuilder.curVeloxExpr.projectionsVec.push_back(projection);
                                                // avoid repeated projectionMap
                                                auto it = queryBuilder.projNameMap.find(projectionName);
                                                if (it == queryBuilder.projNameMap.end())
                                                    queryBuilder.projNameMap.emplace(projectionName, projectionList[0]);
                                            }
                                        }
                                    } else if (headName == "By") {
                                        std::visit(
                                                [&queryBuilder](auto &&argument) {
                                                    return bossExprToVeloxBy(argument, queryBuilder);
                                                },
                                                argument.getArgument());
                                    } else if (headName == "Sum") {
                                        auto aName = fmt::format("a{}", queryBuilder.aggrNameMap.size());
                                        aggrPair aggregation;
                                        aggregation.op = "sum";
                                        aggregation.oldName = tmpArhStr;
                                        aggregation.newName = aName;
                                        queryBuilder.curVeloxExpr.aggregatesVec.push_back(aggregation);
                                        // new implicit name for maybe later orderBy
                                        queryBuilder.aggrNameMap.emplace(tmpArhStr, aName);
                                        queryBuilder.curVeloxExpr.orderBy = true;
                                        if (std::find(queryBuilder.curVeloxExpr.selectedColumns.begin(),
                                                      queryBuilder.curVeloxExpr.selectedColumns.end(),
                                                      tmpArhStr) == queryBuilder.curVeloxExpr.selectedColumns.end())
                                            queryBuilder.curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                    } else {
                                        std::visit([&queryBuilder](auto &&argument) {
                                                       return bossExprToVelox(argument, queryBuilder);
                                                   },
                                                   argument.getArgument());
                                    }
                                }
                            };
                            process(expression.getArguments());
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                (Expression::SuperType const &) expression);
    }

    boss::Expression Engine::evaluate(boss::Expression &&e) {
        QueryBuilder queryBuilder;
        bossExprToVelox(e, queryBuilder);

        queryBuilder.getFileColumnNamesMap();
        queryBuilder.reformVeloxExpr();
        auto planPtr = queryBuilder.getVeloxPlanBuilder();
        std::unique_ptr<TaskCursor> taskCursor;
        auto results = runQuery(planPtr, taskCursor);

        printResults({results});
        std::cout << std::endl;

        ExpressionSpanArguments newSpans;
        for (int i = 0; i < results->childrenSize(); ++i) {
            newSpans.emplace_back(veloxtoSpan(results->childAt(i)));
        }

        auto bossResults = boss::expressions::ExpressionArguments();
        bossResults.push_back(ComplexExpression("List"_, {}, {}, std::move(newSpans)));
        return ComplexExpression("List"_, std::move(bossResults));
    }

} // namespace boss::engines::velox

static auto &enginePtr(bool initialise = true) {
    static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
    if (!engine && initialise) {
        engine.reset(new boss::engines::velox::Engine());
        functions::prestosql::registerAllScalarFunctions();
        aggregate::prestosql::registerAllAggregateFunctions();
        parse::registerTypeResolver();
    }
    return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
    static std::mutex m;
    std::lock_guard lock(m);
    auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
    return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
