#include "BOSSVeloxEngine.hpp"
#include "CommonUtilities.h"

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

//#define DebugInfo
//#define USE_NEW_TABLE_FORMAT

using std::endl;
using std::to_string;
using std::function;
using std::get;
using std::is_invocable_v;
using std::move;
using std::unordered_map;
using std::string_literals::operator""s;

namespace boss::engines::velox {
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
            std::stringstream output;
            output << "unexpected Projection type:";
            for(auto const& arg : input) {
              output << " " << arg;
            }
            throw std::runtime_error(output.str());
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
                            auto name = a.getName();
                            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                            queryBuilder.add_selectedColumns(name);
                            queryBuilder.add_tmpFieldFilter(name, cName);
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
                            auto name = a.getName();
                            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                            queryBuilder.add_selectedColumns(name);
                            projectionList.push_back(name);
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
                            auto name = a.getName();
                            std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                            if (queryBuilder.curVeloxExpr.orderBy) {
                                if (strcasecmp(name.c_str(), "desc") == 0) {
                                    auto it = queryBuilder.curVeloxExpr.orderByVec.end() - 1;
                                    *it = *it + " DESC";
                                } else {
                                    auto it = queryBuilder.aggrNameMap.find(name);
                                    if (it != queryBuilder.aggrNameMap.end()) {
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(it->second);
                                    } else {
                                        queryBuilder.curVeloxExpr.orderByVec.push_back(name);
                                    }
                                }
                            } else {
                                queryBuilder.add_selectedColumns(name);
                                queryBuilder.curVeloxExpr.groupingKeysVec.push_back(name);
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

#ifdef SUPPORT_RADIX_JOINS
    std::vector<BufferPtr> getRadixPartition(Expression&& expression) {
      auto e = transformDynamicsToSpans(std::get<ComplexExpression>(std::move(expression)));
      auto [head, statics, dynamics, spans] = std::move(e).decompose();
      return getIndices(std::move(spans));
    }
#endif // SUPPORT_RADIX_JOINS

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

#ifdef SUPPORT_RADIX_JOINS
                            if(headName == "RadixPartitions") {
                              auto it = std::move_iterator(dynamics.begin());
                              auto it_end = std::move_iterator(dynamics.end());
                              bossExprToVelox(std::move(*it++), queryBuilder);
                              for(; it != it_end; ++it) {
                                auto indices = getRadixPartition(std::move(*it));
                                queryBuilder.curVeloxExpr.radixPartitions.emplace_back(
                                  std::accumulate(indices.begin(), indices.end(), 0,
                                        [](size_t total, auto const& indicesBufferPtr) {
                                        //std::cout << "buffer size: "
                                        //        << indicesBufferPtr->size()
                                        //        << std::endl;
                                        return total + indicesBufferPtr->size() / sizeof(int32_t);
                                      }));
                                //std::cout << "radixPartitions push "
                                //          << queryBuilder.curVeloxExpr.radixPartitions.back()
                                //          << std::endl;
                                queryBuilder.curVeloxExpr.indicesVec.insert(
                                    queryBuilder.curVeloxExpr.indicesVec.end(),
                                    std::move_iterator(indices.begin()),
                                    std::move_iterator(indices.end()));
                              }
                              return;
                            }
#endif // SUPPORT_RADIX_JOINS

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
      if(e.getHead().getName() == "Set") {
        if(std::get<Symbol>(e.getDynamicArguments()[0]) == "maxThreads"_) {
          maxThreads = std::get<int32_t>(e.getDynamicArguments()[1]);
        }
        if(std::get<Symbol>(e.getDynamicArguments()[0]) == "NumSplits"_) {
          numSplits = std::get<int32_t>(e.getDynamicArguments()[1]);
        }
        if(std::get<Symbol>(e.getDynamicArguments()[0]) == "HashAdaptivityEnabled"_) {
          hashAdaptivityEnabled = std::get<bool>(e.getDynamicArguments()[1]);
        }
        return true;
      }

      auto pool = memory::MemoryManager::getInstance()->addLeafPool();

      {
        static std::mutex m;
        std::lock_guard const lock(m);
        pools_.push_back(pool); // TODO: ideally handle lifetime through the spans
                                // (i.e. destroy once no span refers to it)
      }

      auto params = std::make_unique<CursorParameters>();
      params->maxDrivers = maxThreads;
      params->copyResult = false;
      std::shared_ptr<folly::Executor> executor = nullptr;
      if(maxThreads < 2) {
        params->singleThreaded = true;
      } else {
        executor = std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency());
      }
      params->queryCtx = std::make_shared<core::QueryCtx>(
          executor.get(), core::QueryConfig{std::unordered_map<std::string, std::string>{
                               {core::QueryConfig::kHashAdaptivityEnabled,
                                hashAdaptivityEnabled ? "true" : "false"}}});
      std::unique_ptr<TaskCursor> cursor;

      boss::expressions::ExpressionArguments columns;
      auto evalAndAddOutputSpans = [&, this](auto&& e) {
        QueryBuilder queryBuilder(*pool.get());
        bossExprToVelox(std::move(e), queryBuilder);
        if(queryBuilder.tableCnt != 0) {
          queryBuilder.getFileColumnNamesMap();
          queryBuilder.reformVeloxExpr();
          std::vector<core::PlanNodeId> scanIds;
          params->planNode = queryBuilder.getVeloxPlanBuilder(scanIds);
          auto results = veloxRunQueryParallel(*params, cursor, scanIds, numSplits);
          if(!cursor) {
            throw std::runtime_error("Query terminated with error");
          }
#ifdef DebugInfo
          veloxPrintResults(results);
          std::cout << std::endl;
          auto& task = cursor->task();
          auto const& stats = task->taskStats();
          std::cout << printPlanWithStats(*planPtr, stats, false) << std::endl;
    #endif
          if(!results.empty()) {
            for(auto& result : results) {
              // make sure that lazy vectors are computed
              result->loadedVector();
              // copy the result such that it is own by *OUR* memory pool
              // (this also ensure to flatten vectors wrapped in a dictionary)
              auto copy =
                  BaseVector::create<RowVector>(result->type(), result->size(), pool.get());
              copy->copy(result.get(), 0, 0, result->size());
              result = std::move(copy);
            }
            auto const& rowType = dynamic_cast<const RowType*>(results[0]->type().get());
            for(int i = 0; i < results[0]->childrenSize(); ++i) {
              ExpressionSpanArguments spans;
              for(auto& result : results) {
                spans.emplace_back(veloxtoSpan(result->childAt(i)));
              }
              auto const& name = rowType->nameOf(i);
#ifdef USE_NEW_TABLE_FORMAT
              columns.emplace_back(ComplexExpression(Symbol(name), {}, {}, std::move(spans)));
#else
              boss::expressions::ExpressionArguments args;
              args.emplace_back(Symbol(name));
              args.emplace_back(ComplexExpression("List"_, {}, {}, std::move(spans)));
              columns.emplace_back(ComplexExpression("Column"_, std::move(args)));
#endif // USE_NEW_TABLE_FORMAT
            }
          }
        }
      };

      if(e.getHead().getName() == "Union") {
        auto [_, statics, dynamics, spans] = std::move(e).decompose();
        for(auto&& arg : dynamics) {
          evalAndAddOutputSpans(std::move(arg));
        }
      } else {
        evalAndAddOutputSpans(std::move(e));
      }
      return ComplexExpression("Table"_, std::move(columns));
    }

    Engine::Engine() {
      static bool firstInitialization = true;
      if(firstInitialization) {
        firstInitialization = false;
        // this init is required only when the engine is first loaded (not every engine reset):
        memory::MemoryManager::initialize({});
        functions::prestosql::registerAllScalarFunctions();
        aggregate::prestosql::registerAllAggregateFunctions();
        parse::registerTypeResolver();
        FLAGS_velox_exception_user_stacktrace_enabled = true;
      }
      auto bossConnector =
          connector::getConnectorFactory(boss::engines::velox::BossConnectorFactory::kBossConnectorName)
              ->newConnector(boss::engines::velox::kBossConnectorId, nullptr);
      connector::registerConnector(bossConnector);
    }

    Engine::~Engine() { connector::unregisterConnector(boss::engines::velox::kBossConnectorId); }

} // namespace boss::engines::velox

static auto& enginePtr(bool initialise = true) {
  static std::mutex m;
  std::lock_guard const lock(m);
  static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
  if(!engine && initialise) {
    engine.reset(new boss::engines::velox::Engine());
  }
  return engine;
}

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  auto* r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
}

extern "C" void reset() { enginePtr(false).reset(nullptr); }
