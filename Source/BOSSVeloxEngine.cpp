#include "BOSSVeloxEngine.hpp"
#include "BOSSCoreTmp.h"

#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/exec/PlanNodeStats.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

#include <any>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <ExpressionUtilities.hpp>
#include <cstring>
#include <iostream>
#include <regex>
#include <sstream>
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
    using SpanInputs = std::variant<std::vector<std::int32_t>, vector<std::int64_t>, vector<std::double_t>,
            vector<std::string>, vector<Symbol>>;
} // namespace boss

namespace boss::engines::velox {
    using ComplexExpression = boss::expressions::ComplexExpression;
    template<typename... T>
    using ComplexExpressionWithStaticArguments =
            boss::expressions::ComplexExpressionWithStaticArguments<T...>;
    using Expression = boss::expressions::Expression;
    using ExpressionArguments = boss::expressions::ExpressionArguments;
    using ExpressionSpanArguments = boss::expressions::ExpressionSpanArguments;
    using ExpressionSpanArgument = boss::expressions::ExpressionSpanArgument;
    using expressions::generic::ArgumentWrapper;
    using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;


    int32_t dateToInt32(const std::string &str) {
        std::istringstream iss;
        iss.str(str);
        struct std::tm tm = {};
        iss >> std::get_time(&tm, "%Y-%m-%d");
        auto t = std::mktime(&tm);
        return (int32_t) std::chrono::duration_cast<std::chrono::days>(
                std::chrono::system_clock::from_time_t(t).time_since_epoch())
                .count();
    }

    void yearToInt32(std::vector<std::string> &projectionList) {
        auto out = fmt::format("cast(((cast({} AS DOUBLE) + 719563.285) / 365.265) AS INTEGER)",
                               projectionList.back());
        projectionList.pop_back();
        projectionList.pop_back();
        projectionList.push_back(out);
    }

    Expression dateProcess(Expression &&e) {
        return visit(boss::utilities::overload(
                             [](ComplexExpression &&e) -> Expression {
                                 auto head = e.getHead();
                                 if (head.getName() == "DateObject") {
                                     auto argument = e.getArguments().at(0);
                                     std::stringstream out;
                                     out << argument;
                                     auto dateString = out.str().substr(1, 10);
                                     return dateToInt32(dateString);
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
                                auto *ptr = input.data();
                                auto size = input.size();
                                spanReferenceCounter.add(ptr, [v = std::move(input)]() {});
                                return boss::Span<Element>(ptr, size,
                                                           [ptr]() { spanReferenceCounter.remove(ptr); });
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

    template<typename T>
    VectorPtr spanToVelox(boss::Span<T> &&span, memory::MemoryPool *pool, BufferPtr indices = nullptr) {
        BossArray bossArray(span.size(), span.begin(), std::move(span));

        auto createDictVector = [](BufferPtr &indices, auto flatVecPtr) {
            auto indicesSize = indices->size() / 4;
            return BaseVector::wrapInDictionary(
                    BufferPtr(nullptr), indices, indicesSize, std::move(flatVecPtr));
        };

        auto createVeloxVector = [&](auto bossType) {
            auto flatVecPtr = importFromBossAsOwner(bossType, bossArray, pool);
            if (indices == nullptr) {
                return flatVecPtr;
            }
            return createDictVector(indices, flatVecPtr);
        };

        if constexpr (std::is_same_v<T, int32_t>) {
            return createVeloxVector(BossType::bINTEGER);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return createVeloxVector(BossType::bBIGINT);
        } else if constexpr (std::is_same_v<T, double_t>) {
            return createVeloxVector(BossType::bDOUBLE);
        }
    }

    template<typename T>
    boss::Span<const T> createBossSpan(VectorPtr &vec) {
        auto const *data = vec->values()->as<T>();
        auto length = vec->size();
        return boss::Span<const T>(data, length, [v = std::move(vec)]() {});
    }

    ExpressionSpanArgument veloxtoSpan(VectorPtr &&vec) {
        if (vec->typeKind() == TypeKind::INTEGER) {
            return createBossSpan<int32_t>(vec);
        }
        if (vec->typeKind() == TypeKind::BIGINT) {
            return createBossSpan<int64_t>(vec);
        }
        if (vec->typeKind() == TypeKind::DOUBLE) {
            return createBossSpan<double_t>(vec);
        }
        throw std::runtime_error("veloxToSpan: array type not supported: " +
                                 facebook::velox::mapTypeKindToName(vec->typeKind()));
    }

    bool cmpFunCheck(const std::string &input) {
        static std::vector<std::string> const cmpFun{"Greater", "Equal", "StringContainsQ"};
        for (const auto &cmpOp: cmpFun) {
            if (input == cmpOp) {
                return true;
            }
        }
        return false;
    }

    void formatVeloxProjection(std::vector<std::string> &projectionList,
                               std::unordered_map<std::string, std::string> projNameMap) {
        std::vector<std::string> input(3);
        std::string out;
        for (int i = 0; i < 3; i++) {
            auto tmp = projectionList.back();
            auto it = projNameMap.find(tmp);
            if (it != projNameMap.end()) {
                tmp = it->second;
            }
            input[2 - i] = tmp;
            projectionList.pop_back();
        }

        if (input[0] == "Multiply" || input[0] == "Times") {
            out = fmt::format("({}) * ({})", input[1], input[2]);
        } else if (input[0] == "Plus") {
            out = fmt::format("({}) + ({})", input[1], input[2]);
        } else if (input[0] == "Minus" || input[0] == "Subtract") {
            out = fmt::format("({}) - ({})", input[1], input[2]);
        } else {
            throw std::runtime_error("unexpected Projection type");
        }
        projectionList.push_back(out);
    }

    // translate selection and join
    void bossExprToVeloxFilter_Join(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            queryBuilder.add_tmpFieldFilter(to_string(a), cValue);
                        },
                        [&](char const *a) {
                            queryBuilder.add_tmpFieldFilter(a, cValue);
                        },
                        [&](Symbol const &a) {
                            queryBuilder.add_selectedColumns(a.getName());
                            queryBuilder.add_tmpFieldFilter(a.getName(), cName);
                        },
                        [&](std::string const &a) {
                            queryBuilder.add_tmpFieldFilter("'" + a + "'", cValue);
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (cmpFunCheck(headName)) { // field filter or join op pre-process
                                queryBuilder.tmpFieldFilter.clear();
                                queryBuilder.tmpFieldFilter.opName = headName;
                            }

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            for (auto &argument: dynamics) {
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                std::stringstream out;
                                out << argument;
                                auto tmpArhStr = out.str();

                                //for list join
                                if (headName == "List") {
                                    queryBuilder.add_selectedColumns(tmpArhStr);
                                    if (!queryBuilder.tmpJoinPairList.leftFlag) {
                                        queryBuilder.tmpJoinPairList.leftKeys.emplace_back(tmpArhStr);
                                    } else {
                                        queryBuilder.tmpJoinPairList.rightKeys.emplace_back(tmpArhStr);
                                    }
                                    continue;
                                }

                                if (tmpArhStr.substr(0, 10) == "DateObject") {
                                    auto dateString = tmpArhStr.substr(12, 10);
                                    queryBuilder.add_tmpFieldFilter(to_string(dateToInt32(dateString)), cValue);
                                    continue;
                                }
                                if (tmpArhStr.substr(0, 4) == "Date") {
                                    auto dateString = tmpArhStr.substr(6, 10);
                                    queryBuilder.add_tmpFieldFilter(to_string(dateToInt32(dateString)), cValue);
                                    continue;
                                }

                                bossExprToVeloxFilter_Join(std::move(argument), queryBuilder);
                            }
                            queryBuilder.postTransFilter_Join(headName);
                        }),
                std::move(expression));
    }

    // translate projection and partialAggregation
    void bossExprToVeloxProj_PartialAggr(Expression &&expression, std::vector<std::string> &projectionList,
                                         QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](auto a) {
                            projectionList.push_back(to_string(a));
                        },
                        [&](char const *a) {
                            projectionList.emplace_back(a);
                        },
                        [&](Symbol const &a) {
                            queryBuilder.add_selectedColumns(a.getName());
                            projectionList.push_back(a.getName());
                        },
                        [&](std::string const &a) {
                            projectionList.push_back(a);
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            projectionList.push_back(headName);

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            for (auto &argument: dynamics) {
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                if (headName == "Sum" || headName == "Avg" || headName == "Count") {
                                    std::stringstream out;
                                    out << argument;
                                    auto tmpArhStr = out.str();
                                    auto str = headName;
                                    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
                                    aggrPair const aggregation(str, tmpArhStr, "");
                                    queryBuilder.curVeloxExpr.aggregatesVec.push_back(aggregation);
                                    queryBuilder.add_selectedColumns(tmpArhStr);
                                    continue;
                                }

                                bossExprToVeloxProj_PartialAggr(std::move(argument), projectionList, queryBuilder);

                                if (headName == "Year") {
                                    yearToInt32(projectionList);
                                }
                            }
                            if (projectionList.size() >= 3) {
                                formatVeloxProjection(projectionList, queryBuilder.projNameMap);
                            }
                        }),
                std::move(expression));
    }

    // translate order_by and group_by
    void bossExprToVeloxBy(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](Symbol const &a) {
                            if (queryBuilder.curVeloxExpr.orderBy) {
                                if (strcasecmp(a.getName().c_str(), "desc") == 0) {
                                    auto it = queryBuilder.curVeloxExpr.orderByVec.end() - 1;
                                    *it = *it + " DESC";
                                } else {
                                    auto it = queryBuilder.aggrNameMap.find(a.getName());
                                    if (it != queryBuilder.aggrNameMap.end()) {
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(it->second);
                                    } else {
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(a.getName());
                                    }
                                }
                            } else {
                                queryBuilder.add_selectedColumns(a.getName());
                                queryBuilder.curVeloxExpr.groupingKeysVec.push_back(a.getName());
                            }
                        },
                        [&](ComplexExpression &&expression) {
#ifdef DebugInfo
                            auto headName = expression.getHead().getName();
                            std::cout << "headName  " << headName << endl;
#endif
                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();
                            for (auto &argument: dynamics) {
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                bossExprToVeloxBy(std::move(argument), queryBuilder);
                            }
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                std::move(expression));
    }

// "Table"_("Column"_("Id"_, "List"_(1, 2, 3), "Column"_("Value"_ "List"_(0.1, 10.0, 5.2)))
    std::unordered_map<std::string, TypePtr> getColumns(
            ComplexExpression &&expression, std::vector<BufferPtr> indicesVec,
            std::vector<RowVectorPtr> &rowDataVec, RowTypePtr &tableSchema, std::vector<size_t> &spanRowCountVec, memory::MemoryPool *pool) {
        ExpressionArguments columns = std::move(expression).getArguments();
        std::vector<std::string> colNameVec;
        std::vector<std::shared_ptr<const Type>> colTypeVec;
        std::vector<std::vector<VectorPtr>> colDataListVec;
        std::unordered_map<std::string, TypePtr> fileColumnNamesMap(columns.size());

        std::for_each(
                std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
                [&colNameVec, &colTypeVec, &colDataListVec, pool, &fileColumnNamesMap, &indicesVec](
                        auto &&columnExpr) {
                    auto column = get < ComplexExpression > (std::forward<decltype(columnExpr)>(columnExpr));
                    auto [head, unused_, dynamics, spans] = std::move(column).decompose();
                    auto columnName = get < Symbol > (std::move(dynamics.at(0)));
                    auto dynamic = get < ComplexExpression > (std::move(dynamics.at(1)));
                    auto list = transformDynamicsToSpans(std::move(dynamic));
                    auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
                    if (listSpans.empty()) {
                        return;
                    }

                    int numSpan = 0;
                    std::vector<VectorPtr> colDataVec;
                    for (auto &subSpan: listSpans) {
                        BufferPtr indices = nullptr;
                        if (!indicesVec.empty()) {
                            indices = indicesVec[numSpan++];
                        }
                        VectorPtr subColData = std::visit(
                                [pool, indices]<typename T>(boss::Span<T> &&typedSpan) -> VectorPtr {
                                    if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                                                  std::is_same_v<T, double_t>) {
                                        return spanToVelox<T>(std::move(typedSpan), pool, indices);
                                    } else {
                                        throw std::runtime_error("unsupported column type in Select");
                                    }
                                },
                                std::move(subSpan));
                        colDataVec.push_back(std::move(subColData));
                    }
                    auto columnType = colDataVec[0]->type();
                    colNameVec.emplace_back(columnName.getName());
                    colTypeVec.emplace_back(columnType);
                    fileColumnNamesMap.insert(std::make_pair(columnName.getName(), columnType));
                    colDataListVec.push_back(std::move(colDataVec));
                });

        auto listSize = colDataListVec[0].size();
        for (auto i = 0; i < listSize; i++) {
            spanRowCountVec.push_back(colDataListVec[0][i]->size());
            std::vector<VectorPtr> rowData;
            for (auto j = 0; j < columns.size(); j++) {
                assert(colDataListVec[j].size() == listSize);
                rowData.push_back(std::move(colDataListVec[j][i]));
            }
            auto rowVector = makeRowVectorNoCopy(colNameVec, rowData, pool);
            rowDataVec.push_back(std::move(rowVector));
        }

        tableSchema = TypeFactory<TypeKind::ROW>::create(std::move(colNameVec), std::move(colTypeVec));
        return fileColumnNamesMap;
    }

    std::vector<BufferPtr> getIndices(ExpressionSpanArguments &&listSpans) {
        if (listSpans.empty()) {
            throw std::runtime_error("get index error");
        }

        std::vector<BufferPtr> indicesVec;
        for (auto &subSpan: listSpans) {
            BufferPtr indexData = std::visit(
                    []<typename T>(boss::Span<T> &&typedSpan) -> BufferPtr {
                        if constexpr (std::is_same_v<T, int32_t>) {
                            BossArray bossIndices(typedSpan.size(), typedSpan.begin(), std::move(typedSpan));
                            return importFromBossAsOwnerBuffer(bossIndices);
                        } else {
                            throw std::runtime_error("index type error");
                        }
                    },
                    std::move(subSpan));
            indicesVec.push_back(std::move(indexData));
        }
        return indicesVec;
    }

    void QueryBuilder::getTableMeta(ComplexExpression &&expression) {
        if (!curVeloxExpr.tableName.empty()) {
            veloxExprList.push_back(curVeloxExpr);
            curVeloxExpr.clear();
        }
        curVeloxExpr.tableName = fmt::format("Table{}", tableCnt++); // indicate table names
        curVeloxExpr.fileColumnNamesMap = getColumns(std::move(expression),
                                                     curVeloxExpr.indicesVec,
                                                     curVeloxExpr.rowDataVec,
                                                     curVeloxExpr.tableSchema,
                                                     curVeloxExpr.spanRowCountVec,
                                                     pool_.get());
    }

    // translate main
    void bossExprToVelox(Expression &&expression, QueryBuilder &queryBuilder) {
        std::visit(
                boss::utilities::overload(
                        [&](std::int32_t a) {
                            queryBuilder.curVeloxExpr.limit = a;
                        },
                        [&](ComplexExpression &&expression) {
                            auto headName = expression.getHead().getName();
#ifdef DebugInfo
                            std::cout << "headName  " << headName << endl;
#endif
                            if (headName == "Table") {
                                // traverse for a new plan builder, triggered by Table head
                                queryBuilder.getTableMeta(std::move(expression));
                                return;
                            }

                            std::string projectionName;
                            std::vector<std::string> lastProjectionsVec;
                            // save the latest ProjectionsVec to restore for AS - aggregation
                            if (headName == "As") {
                                lastProjectionsVec = queryBuilder.curVeloxExpr.projectionsVec;
                                queryBuilder.curVeloxExpr.projectionsVec.clear();
                            }

                            auto [head, statics, dynamics, oldSpans] = std::move(expression).decompose();

                            if (headName == "Gather") {
                                auto it_start = std::move_iterator(dynamics.begin());
                                queryBuilder.curVeloxExpr.indicesVec = getIndices(std::move(oldSpans));
                                bossExprToVelox(std::move(*it_start), queryBuilder);
                                return;
                            }

                            int count = 0;
                            for (auto &argument: dynamics) {
#ifdef DebugInfo
                                std::cout << "argument  " << argument << endl;
#endif
                                auto toString = [](auto &&arg) {
                                    std::stringstream out;
                                    out << arg;
                                    return out.str();
                                };

                                if (headName == "Where") {
                                    bossExprToVeloxFilter_Join(std::move(argument), queryBuilder);
                                    if (!queryBuilder.curVeloxExpr.tmpFieldFiltersVec.empty()) {
                                        queryBuilder.formatVeloxFilter_Join();
                                    }
                                } else if (headName == "As") {
                                    if (count % 2 == 0) {
                                        projectionName = toString(argument);
                                    } else {
                                        std::vector<std::string> projectionList;
                                        bossExprToVeloxProj_PartialAggr(std::move(argument), projectionList,
                                                                        queryBuilder);
                                        queryBuilder.postTransProj_PartialAggr(projectionList, lastProjectionsVec,
                                                                               projectionName);
                                    }
                                } else if (headName == "By") {
                                    bossExprToVeloxBy(std::move(argument), queryBuilder);
                                } else if (headName == "Sum") {
                                    queryBuilder.postTransSum(toString(argument));
                                } else {
                                    bossExprToVelox(std::move(argument), queryBuilder);
                                }
                                count++;
                            }
                        },
                        [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
                std::move(expression));
    }

    boss::Expression Engine::evaluate(boss::Expression &&e) {
        if (std::holds_alternative<ComplexExpression>(e)) {
            return evaluate(std::get<ComplexExpression>(std::move(e)));
        }
        return std::move(e);
    }

    boss::Expression Engine::evaluate(boss::ComplexExpression&& e) {
        QueryBuilder queryBuilder;
        bossExprToVelox(std::move(e), queryBuilder);
        if(queryBuilder.tableCnt != 0) {
            queryBuilder.getFileColumnNamesMap();
            queryBuilder.reformVeloxExpr();
            std::vector<core::PlanNodeId> scanIds;
            auto planPtr = queryBuilder.getVeloxPlanBuilder(scanIds);
            params.planNode = planPtr;
            params.maxDrivers = 4;
            const int numSplits = 5;
            auto results = runNew(params, cursor, scanIds, numSplits);
//            auto results = run2(params, cursor, scanIds, numSplits);
//            auto results = veloxRunQuery(params, cursor);
            if(!cursor) {
                throw std::runtime_error("Query terminated with error");
            }
//#ifdef DebugInfo
            veloxPrintResults(results);
            std::cout << std::endl;
#ifdef DebugInfo
            auto task = cursor->task();
            const auto stats = task->taskStats();
            std::cout << printPlanWithStats(*planPtr, stats, false) << std::endl;
#endif
            ExpressionSpanArguments newSpans;
            if (!results.empty()) {
                for (int i = 0; i < results[0]->childrenSize(); ++i) {
                    for (auto &result: results) {
                        newSpans.emplace_back(veloxtoSpan(std::move(result->childAt(i))));
                    }
                }
            }

            auto bossResults = boss::expressions::ExpressionArguments();
            bossResults.push_back(ComplexExpression("List"_, {}, {}, std::move(newSpans)));
            return ComplexExpression("List"_, std::move(bossResults));
        }
        return std::move(e);
    }

} // namespace boss::engines::velox

static auto &enginePtr(bool initialise = true) {
    static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
    if (!engine && initialise) {
        engine.reset(new boss::engines::velox::Engine());
        engine->executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(
                std::thread::hardware_concurrency());
        engine->params.queryCtx = std::make_shared<core::QueryCtx>(engine->executor_.get());
        functions::prestosql::registerAllScalarFunctions();
        aggregate::prestosql::registerAllAggregateFunctions();
        parse::registerTypeResolver();
        auto bossConnector = connector::getConnectorFactory(
                                 boss::engines::velox::BossConnectorFactory::kBossConnectorName)
                                 ->newConnector(boss::engines::velox::kBossConnectorId, nullptr);
        connector::registerConnector(bossConnector);
    }
    return engine;
}

extern "C" BOSSExpression *evaluate(BOSSExpression *e) {
    static std::mutex m;
    std::lock_guard const lock(m);
    auto *r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
    return r;
}

extern "C" void reset() { enginePtr(false).reset(nullptr); }
