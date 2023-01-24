#include "BOSSVeloxEngine.hpp"
#include "BOSSQueryBuilder.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"
//#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/arrow/Bridge.h"
#include <arrow/api.h>
//#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

//#include "spdlog/cfg/env.h"
//#include "spdlog/sinks/basic_file_sink.h"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <algorithm>

#include <Utilities.hpp>
#include <__tuple>
#include <any>
#include <iterator>
#include <list>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

std::ostream &operator<<(std::ostream &s, std::vector<std::int64_t> const &input /*unused*/) {
  std::for_each(begin(input), prev(end(input)),
                [&s = s << "["](auto &&element) { s << element << ", "; });
  return (input.empty() ? s : (s << input.back())) << "]";
}

#include <ExpressionUtilities.hpp>
#include <arrow/array.h>
#include <cstring>
#include <iostream>
#include <regex>
#include <set>
//#include <spdlog/common.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#define DebugInfo

using std::function;
using std::get;
using std::is_invocable_v;
using std::move;
using std::unordered_map;
using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss {
    using std::vector;
    using SpanInputs = std::variant<vector<bool>, vector<std::int64_t>, vector<std::double_t>,
            vector<std::string>, vector<Symbol>>;
} // namespace boss

template<typename... StaticArgumentTypes>
boss::ComplexExpressionWithStaticArguments<StaticArgumentTypes...> transformDynamicsToSpans(
        boss::ComplexExpressionWithStaticArguments<StaticArgumentTypes...> &&input_) {
  std::vector<boss::SpanInputs> spanInputs;
  auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();
  if (sizeof...(StaticArgumentTypes) + dynamics.size() > 1) {
    for (auto it =
            std::move_iterator(sizeof...(StaticArgumentTypes) == 0 ? next(dynamics.begin()) : dynamics.begin());
         it != std::move_iterator(dynamics.end()); ++it) {
      std::visit(
              [&spanInputs]<typename InputType>(InputType &&argument) {
                  using Type = std::decay_t<InputType>;
                  if constexpr (boss::utilities::isVariantMember<std::vector<Type>,
                          boss::SpanInputs>::value) {
                    if (spanInputs.size() > 0 &&
                        std::holds_alternative<std::vector<Type>>(spanInputs.back())) {
                      std::get<std::vector<Type>>(spanInputs.back()).push_back(argument);
                    } else {
                      spanInputs.push_back(std::vector<Type>{argument});
                    }
                  }
              },
              *it);
    }
    dynamics.erase(next(dynamics.begin()), dynamics.end());
  }

  boss::expressions::generic::ExpressionSpanArgumentsWithAdditionalCustomAtoms<> spans;
  std::transform(std::move_iterator(spanInputs.begin()), std::move_iterator(spanInputs.end()),
                 std::back_inserter(spans), [](auto &&untypedInput) {
              return std::visit(
                      []<typename Element>(std::vector<Element> &&input)
                              -> boss::expressions::ExpressionSpanArguments::value_type {
                          return boss::Span<Element>(std::move(input));
                      },
                      std::move(untypedInput));
          });

  std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
            std::back_inserter(spans));
  return {head, std::move(statics), std::move(dynamics), std::move(spans)};
}

Expression transformDynamicsToSpans(Expression &&input) {
  return std::visit(
          [](auto &&x) -> Expression {
              if constexpr (std::is_same_v<std::decay_t<decltype(x)>, boss::ComplexExpression>)
                return transformDynamicsToSpans(std::move(x));
              else
                return x;
          },
          std::move(input));
}


void ensureTaskCompletion(exec::Task *task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}

void printResults(const std::vector<RowVectorPtr> &results) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for (const auto &vector: results) {
    // Print RowType only once.
    if (printType) {
      std::cout << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for (vector_size_t i = 0; i < vector->size(); ++i) {
      std::cout << vector->toString(i) << std::endl;
    }
  }
}

std::shared_ptr<arrow::Array> toArrow(
        const VectorPtr &vec,
        memory::MemoryPool* pool =
        &facebook::velox::memory::getProcessDefaultMemoryManager().getRoot()){
//        memory::MemoryPool *pool) {
  ArrowSchema schema;
  ArrowArray array;
  exportToArrow(vec, schema);
  exportToArrow(vec, array, pool);
  auto type = arrow::ImportType(&schema);
  auto ans = arrow::ImportArray(&array, type.ValueOrDie());
  return ans.ValueOrDie();
}

VectorPtr toVelox(const arrow::Array &array) {
  ArrowSchema schema;
  ArrowArray data;
  arrow::ExportType(*array.type(), &schema);
  arrow::ExportArray(array, &data);
  auto vec = importFromArrowAsViewer(schema, data);
  schema.release(&schema);
  data.release(&data);
  return vec;
}

class BossBenchmark : public VectorTestBase {
public:
    void initialize() {
      functions::prestosql::registerAllScalarFunctions();
      aggregate::prestosql::registerAllAggregateFunctions();
      parse::registerTypeResolver();
      filesystems::registerLocalFileSystem();
      parquet::registerParquetReaderFactory(parquet::ParquetReaderType::NATIVE);
      dwrf::registerDwrfReaderFactory();
      auto hiveConnector =
              connector::getConnectorFactory(connector::hive::HiveConnectorFactory::kHiveConnectorName)
                      ->newConnector(kHiveConnectorId, nullptr);
      connector::registerConnector(hiveConnector);
    }

    std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(const BossPlan &bossPlan) {
      CursorParameters params;
      params.maxDrivers = 4;
      params.planNode = bossPlan.plan;
      const int numSplitsPerFile = 10;

      bool noMoreSplits = false;
      auto addSplits = [&](exec::Task *task) {
          if (!noMoreSplits) {
            for (const auto &entry: bossPlan.dataFiles) {
              for (const auto &path: entry.second) {
                auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                        path, numSplitsPerFile, bossPlan.dataFileFormat);
                for (const auto &split: splits) {
                  task->addSplit(entry.first, exec::Split(split));
                }
              }
              task->noMoreSplits(entry.first);
            }
          }
          noMoreSplits = true;
      };
      return readCursor(params, addSplits);
    }

    RowVectorPtr makeRowVectorWrap(const std::vector<VectorPtr> &children) {
      return makeRowVector(children);
    }

};

namespace boss::engines::velox {
    using std::move;
    using VeloxExpressionSystem = ExtensibleExpressionSystem<>;
    using AtomicExpression = VeloxExpressionSystem::AtomicExpression;
    using ComplexExpression = VeloxExpressionSystem::ComplexExpression;
    using Expression = VeloxExpressionSystem::Expression;
    using ExpressionArguments = VeloxExpressionSystem::ExpressionArguments;
    using ExpressionSpanArguments = VeloxExpressionSystem::ExpressionSpanArguments;
    using expressions::generic::ArgumentWrapper;
    using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;

    class StatelessOperator {
    public:
        std::map<std::type_index, std::any> functors;
        Expression operator()(boss::ComplexExpression&& e) {
          auto [head, statics, dynamics, spans] = std::move(e).decompose();
          boss::expressions::ExpressionArguments rest{};
          if(dynamics.size() > 1)
            std::copy(std::move_iterator(next(dynamics.begin())), std::move_iterator(dynamics.end()),
                      std::back_inserter(rest));
          Expression dispatchArgument =
                  dynamics.size() > 0 ? std::move(dynamics.front())
                                      : std::visit([](auto& a) -> Expression { return a[0]; }, spans.front());
          if(dynamics.size() == 0) {
            spans[0] = std::visit(
                    [](auto&& span) -> boss::expressions::ExpressionSpanArguments::value_type {
                        return std::move(span).subspan(1);
                    },
                    std::move(spans[0]));
          }
          static_assert(std::tuple_size_v<decltype(statics)> == 0);
          return std::visit(
                  [head = head, statics = statics, dynamics = std::move(rest), spans = spans,
                          this](auto&& argument) mutable -> Expression {
                      typedef std::decay_t<decltype(argument)> ArgType;

                      if constexpr(std::is_same_v<ArgType, std::string> || std::is_same_v<ArgType, double> ||
                                   std::is_same_v<ArgType, long long> ||
                                   std::is_same_v<ArgType, boss::Symbol>) {
                        // TODO: this dispatch mechanism needs to handle functions with multiple static
                        // arguments
                        return std::any_cast<
                                std::function<Expression(boss::ComplexExpressionWithStaticArguments<ArgType> &&)>>(
                                functors.at(typeid(ArgType)))(boss::ComplexExpressionWithStaticArguments<ArgType>(
                                head, {argument}, std::move(dynamics), std::move(spans)));
                      } else if constexpr(std::is_same_v<ArgType, ComplexExpression>) {

                        return std::any_cast<std::function<Expression(boss::ComplexExpression &&)>>(
                                functors.at(typeid(boss::ComplexExpression)))(
                                boss::expressions::ComplexExpressionWithStaticArguments<decltype(argument)>(
                                        head, std::tuple{std::move(argument)}, std::move(dynamics), std::move(spans)));
                      } else {
                        return boss::expressions::ComplexExpressionWithStaticArguments<decltype(argument)>(
                                head, std::tuple{std::move(argument)}, std::move(dynamics), std::move(spans));
                      }
                  },
                  evaluate(std::move(dispatchArgument)));
        }

        template <typename F, typename... Types>
        void registerFunctorForTypes(F f, std::variant<Types...>) {
          (
                  [this, f]() {
                      if constexpr(is_invocable_v<F, boss::ComplexExpressionWithStaticArguments<Types>>)
                        this->functors[typeid(Types)] = std::function<boss::Expression(
                                boss::ComplexExpressionWithStaticArguments<Types> &&)>(f);
                      ;
                  }(),
                  ...);
        }

        template <typename TypedFunctor> StatelessOperator& operator=(TypedFunctor f) {
          registerFunctorForTypes(f, boss::expressions::AtomicExpression{});
          if constexpr(is_invocable_v<TypedFunctor, boss::ComplexExpression>)
            this->functors[typeid(boss::ComplexExpression)] =
                    std::function<boss::Expression(boss::ComplexExpression &&)>(f);
          return *this;
        }
    };

    using ExpressionBuilder = boss::utilities::ExtensibleExpressionBuilder<VeloxExpressionSystem>;

    static ExpressionBuilder operator ""_(const char *name, size_t /*unused*/) {
      return ExpressionBuilder(name);
    };

    using std::set;
    using std::string;
    using std::to_string;
    using std::vector;
    using std::string_literals::operator ""s;
    using std::endl;

    BossBenchmark benchmark;
    std::shared_ptr<BossQueryBuilder> queryBuilder;
    std::vector<FormExpr> veloxExprList;
    FormExpr curVeloxExpr;
    std::unordered_map<std::string, std::string> projNameMap;
    std::unordered_map<std::string, std::string> aggrNameMap;
    std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};

    static bool cmpFunCheck(std::string input) {
      static std::vector<std::string> cmpFun{"Greater", "Equal", "StringContains"};
      for (int i = 0; i < cmpFun.size(); i++) {
        if (input == cmpFun[i]) {
          return 1;
        }
      }
      return 0;
    }

    void mergeGreaterFilter(FiledFilter input) {
      if (input.element[0].type == cName) {
        auto colName = input.element[0].data;
        for (int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
          if (curVeloxExpr.tmpFieldFiltersVec[i].element[0].type == cValue &&
              curVeloxExpr.tmpFieldFiltersVec[i].element[1].type == cName &&
              curVeloxExpr.tmpFieldFiltersVec[i].element[1].data == colName) {
            input.element.push_back(curVeloxExpr.tmpFieldFiltersVec[i].element[0]);
            input.opName = "Between";
            curVeloxExpr.tmpFieldFiltersVec[i] = input;
            return;
          }
        }
        curVeloxExpr.tmpFieldFiltersVec.push_back(input);
      } else if (input.element[1].type == cName) {
        auto colName = input.element[1].data;
        for (int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
          if (curVeloxExpr.tmpFieldFiltersVec[i].element[0].type == cName &&
              curVeloxExpr.tmpFieldFiltersVec[i].element[1].type == cValue &&
              curVeloxExpr.tmpFieldFiltersVec[i].element[0].data == colName) {
            curVeloxExpr.tmpFieldFiltersVec[i].opName = "Between";
            curVeloxExpr.tmpFieldFiltersVec[i].element.push_back(input.element[0]);
            return;
          }
        }
        curVeloxExpr.tmpFieldFiltersVec.push_back(input);
      }
    }

    void formatVeloxFilter_Join() {
      curVeloxExpr.fieldFiltersVec.clear();
      curVeloxExpr.hashJoinVec.clear();
      if (curVeloxExpr.tableName == "tmp") {
        for (auto it = curVeloxExpr.tmpFieldFiltersVec.begin(); it != curVeloxExpr.tmpFieldFiltersVec.end(); ++it) {
          auto const &filter = *it;
          if (filter.opName == "Greater") {
            if (filter.element[0].type == cName && filter.element[1].type == cValue) {
              auto tmp = fmt::format("{} > {}", filter.element[0].data,
                                     filter.element[1].data); // the column name should be on the left side.
              curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            } else if (filter.element[1].type == cName && filter.element[0].type == cValue) {
              auto tmp = fmt::format("{} < {}", filter.element[1].data, filter.element[0].data);
              curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            }
          } else if (filter.opName == "Between") {
            auto tmp = fmt::format("{} between {} and {}", filter.element[0].data, filter.element[1].data,
                                   filter.element[2].data);
            curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
          } else if (filter.opName == "Equal") {
            if (filter.element[0].type == cName && filter.element[1].type == cName) {
              JoinPair tmpJoinPair;
              tmpJoinPair.leftKey = filter.element[0].data;
              tmpJoinPair.rightKey = filter.element[1].data;
              curVeloxExpr.hashJoinVec.push_back(tmpJoinPair);
            } else {
              auto tmp = fmt::format("{} = {}", filter.element[0].data, filter.element[1].data);
              curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            }
          } else if (filter.opName == "StringContains") {
            auto length = filter.element[1].data.size();
            auto tmp = fmt::format("{} like '%{}%'", filter.element[0].data,
                                   filter.element[1].data.substr(1, length - 2));
            curVeloxExpr.remainingFilter = std::move(tmp);
          } else VELOX_FAIL("unexpected Filter type");
        }
      } else {
        auto fileColumnNames = queryBuilder->getFileColumnNames(curVeloxExpr.tableName);
        for (auto it = curVeloxExpr.tmpFieldFiltersVec.begin(); it != curVeloxExpr.tmpFieldFiltersVec.end(); ++it) {
          auto const &filter = *it;
          if (filter.opName == "Greater") {
            if (filter.element[0].type == cName && filter.element[1].type == cValue) {
              auto tmp = fmt::format("{} > {}", filter.element[0].data,
                                     filter.element[1].data); // the column name should be on the left side.
              if (fileColumnNames.find(filter.element[0].data) == fileColumnNames.end())
                curVeloxExpr.filter = std::move(tmp);  // not belong to any field
              else
                curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            } else if (filter.element[1].type == cName && filter.element[0].type == cValue) {
              auto tmp = fmt::format("{} < {}", filter.element[1].data, filter.element[0].data);
              if (fileColumnNames.find(filter.element[1].data) == fileColumnNames.end())
                curVeloxExpr.filter = std::move(tmp);
              else
                curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            }
          } else if (filter.opName == "Between") {
            auto tmp = fmt::format("{} between {} and {}", filter.element[0].data, filter.element[1].data,
                                   filter.element[2].data);
            curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
          } else if (filter.opName == "Equal") {
            if (filter.element[0].type == cName && filter.element[1].type == cName) {
              JoinPair tmpJoinPair;
              tmpJoinPair.leftKey = filter.element[0].data;
              tmpJoinPair.rightKey = filter.element[1].data;
              curVeloxExpr.hashJoinVec.push_back(tmpJoinPair);
            } else {
              auto tmp = fmt::format("{} = {}", filter.element[0].data, filter.element[1].data);
              curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            }
          } else if (filter.opName == "StringContains") {
            auto length = filter.element[1].data.size();
            auto tmp = fmt::format("{} like '%{}%'", filter.element[0].data,
                                   filter.element[1].data.substr(1, length - 2));
            curVeloxExpr.remainingFilter = std::move(tmp);
          } else VELOX_FAIL("unexpected Filter type");
        }
      }
    }

    static void formatVeloxProjection(std::vector<std::string> &projectionList,
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

    void bossExprToVeloxFilter_Join(Expression const &expression) {
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
                      [&](std::vector<int64_t> values) {
                          ExpressionSpanArguments vs;
                          vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
                          bossExprToVeloxFilter_Join(ComplexExpression("List"_, {}, {}, std::move(vs)));
                      },
                      [&](char const *a) {
                          AtomicExpr element;
                          element.data = a;
                          element.type = cValue;
                          tmpFieldFilter.element.emplace_back(element);
                      },
                      [&](Symbol const &a) {
                          if (std::find(curVeloxExpr.selectedColumns.begin(),
                                        curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                        a.getName()) == curVeloxExpr.selectedColumns.end())
                            curVeloxExpr.selectedColumns.push_back(a.getName());
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
                                  AtomicExpr element;
                                  element.data = "'" + dateString + "'" + "::DATE";
                                  element.type = cValue;
                                  tmpFieldFilter.element.emplace_back(element);
                                  continue;
                                }
                                if (tmpArhStr.substr(0, 4) == "Date") {
                                  std::string dateString = tmpArhStr.substr(6, 10);
                                  AtomicExpr element;
                                  element.data = "'" + dateString + "'" + "::DATE";
                                  element.type = cValue;
                                  tmpFieldFilter.element.emplace_back(element);
                                  continue;
                                }
                                if (headName == "List") {
                                  if (std::find(curVeloxExpr.selectedColumns.begin(),
                                                curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                                tmpArhStr) == curVeloxExpr.selectedColumns.end())
                                    curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                  if (!tmpJoinPairList.leftFlag)
                                    tmpJoinPairList.leftKeys.emplace_back(tmpArhStr);
                                  else
                                    tmpJoinPairList.rightKeys.emplace_back(tmpArhStr);
                                  continue;
                                }

                                std::visit([](auto &&argument) { return bossExprToVeloxFilter_Join(argument); },
                                           argument.getArgument());
                              }
                              if (!tmpFieldFilter.opName.empty()) { // field filter or join op post-process
                                if (headName == "Greater")
                                  mergeGreaterFilter(tmpFieldFilter);
                                else if ((headName == "Equal" || headName == "StringContains") &&
                                         tmpFieldFilter.element.size() > 0) {
                                  curVeloxExpr.tmpFieldFiltersVec.push_back(tmpFieldFilter);
                                }
                              }
                              if (headName == "List") {
                                if (!tmpJoinPairList.leftFlag)
                                  tmpJoinPairList.leftFlag = true;
                                else {
                                  curVeloxExpr.hashJoinListVec.push_back(tmpJoinPairList);
                                  tmpJoinPairList.clear();
                                }
                              }
                          };
                          process(expression.getArguments());
                      }),
              (Expression::SuperType const &) expression);
    }

    void bossExprToVeloxProj_PartialAggr(Expression const &expression, std::vector<std::string> &projectionList) {
      std::visit(
              boss::utilities::overload(
                      [&](auto a) {
                          projectionList.push_back(to_string(a));
                      },
                      [&](std::vector<bool>::reference a) {
                      },
                      [&](std::vector<int64_t> values) {
                          ExpressionSpanArguments vs;
                          vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
                          bossExprToVeloxProj_PartialAggr(ComplexExpression("List"_, {}, {}, std::move(vs)),
                                                          projectionList);
                      },
                      [&](char const *a) {
                          projectionList.push_back(a);
                      },
                      [&](Symbol const &a) {
                          if (std::find(curVeloxExpr.selectedColumns.begin(),
                                        curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                        a.getName()) == curVeloxExpr.selectedColumns.end())
                            curVeloxExpr.selectedColumns.push_back(a.getName());
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
                                  curVeloxExpr.aggregatesVec.push_back(aggregation);
                                  if (std::find(curVeloxExpr.selectedColumns.begin(),
                                                curVeloxExpr.selectedColumns.end(),
                                                tmpArhStr) == curVeloxExpr.selectedColumns.end())
                                    curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                  continue;
                                }

                                std::visit(
                                        [&projectionList](auto &&argument) {
                                            return bossExprToVeloxProj_PartialAggr(argument, projectionList);
                                        },
                                        argument.getArgument());
                                if (headName == "Year") {
                                  auto out = fmt::format("year({})", projectionList.back());
                                  projectionList.pop_back();
                                  projectionList.pop_back();
                                  projectionList.push_back(out);
                                }
                              }
                              if (projectionList.size() >= 3) { // field filter or join op post-process
                                formatVeloxProjection(projectionList, projNameMap);
                              }
                          };
                          process(expression.getArguments());
                      }),
              (Expression::SuperType const &) expression);
    }

    void bossExprToVeloxBy(Expression const &expression) {
      std::visit(
              boss::utilities::overload(
                      [&](char const *a) {
                      },
                      [&](Symbol const &a) {
                          if (curVeloxExpr.orderBy) {
                            if (!strcasecmp(a.getName().c_str(), "desc")) {
                              auto it = curVeloxExpr.orderByVec.end() - 1;
                              *it = *it + " DESC";
                            } else {
                              auto it = aggrNameMap.find(a.getName());
                              if (it != aggrNameMap.end())
                                curVeloxExpr.orderByVec.push_back(it->second);
                              else
                                curVeloxExpr.orderByVec.push_back(a.getName());
                            }
                          } else {
                            if (std::find(curVeloxExpr.selectedColumns.begin(),
                                          curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                                          a.getName()) == curVeloxExpr.selectedColumns.end())
                              curVeloxExpr.selectedColumns.push_back(a.getName());
                            curVeloxExpr.groupingKeysVec.push_back(a.getName());
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
                                        [](auto &&argument) {
                                            return bossExprToVeloxBy(argument);
                                        },
                                        argument.getArgument());
                              }
                          };
                          process(expression.getArguments());
                      },
                      [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
              (Expression::SuperType const &) expression);
    }

    void bossExprToVelox(Expression const &expression) {
      std::visit(
              boss::utilities::overload(
                      [&](bool a) {
                      },
                      [&](std::vector<bool>::reference a) {
                      },
                      [&](std::int64_t a) {
                          curVeloxExpr.limit = a;
                      },
                      [&](std::vector<int64_t> values) {
                          ExpressionSpanArguments vs;
                          vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
                          bossExprToVelox(ComplexExpression("List"_, {}, {}, std::move(vs)));
                      },
                      [&](char const *a) {
                      },
                      [&](std::double_t a) {
                      },
                      [&](Symbol const &a) {
                          if (!curVeloxExpr.tableName.empty()) // traverse for a new plan builder, triggerred by select and join
                          {
                            veloxExprList.push_back(curVeloxExpr);
                            curVeloxExpr.clear();
                          }
                          curVeloxExpr.tableName = a.getName(); // indicate table names
                      },
                      [&](std::string const &a) {
                      },
                      [&](ComplexExpression const &expression) {
                          std::string projectionName;
                          auto headName = expression.getHead().getName();
#ifdef DebugInfo
                          std::cout << "headName  " << headName << endl;
#endif
                          auto process = [&](auto const &arguments) {
                              std::vector<std::string> lastProjectionsVec;
                              if (headName ==
                                  "As") { //save the latest ProjectionsVec to restore for AS - aggregation
                                lastProjectionsVec = curVeloxExpr.projectionsVec;
                                curVeloxExpr.projectionsVec.clear();
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
                                          [](auto &&argument) { return bossExprToVeloxFilter_Join(argument); },
                                          argument.getArgument());
                                  if (curVeloxExpr.tmpFieldFiltersVec.size() > 0)
                                    formatVeloxFilter_Join();
                                } else if (headName == "As") {
                                  if ((it - arguments.begin()) % 2 == 0) {
                                    projectionName = tmpArhStr;
                                  } else {
                                    std::vector<std::string> projectionList;
                                    std::visit(
                                            [&projectionList](auto &&argument) {
                                                return bossExprToVeloxProj_PartialAggr(argument, projectionList);
                                            },
                                            argument.getArgument());

                                    // fill the new name for aggregation
                                    if (!curVeloxExpr.aggregatesVec.empty()) {
                                      auto &aggregation = curVeloxExpr.aggregatesVec.back();
                                      if (aggregation.newName == "") {
                                        aggregation.newName = projectionName;
                                        curVeloxExpr.orderBy = true;
                                      }
                                      curVeloxExpr.projectionsVec = lastProjectionsVec;
                                    } else {
                                      std::string projection;
                                      if (projectionList[0] != projectionName)
                                        projection = fmt::format("{} AS {}", projectionList[0], projectionName);
                                      else
                                        projection = projectionList[0];
                                      curVeloxExpr.projectionsVec.push_back(projection);
                                      // avoid repeated projectionMap
                                      auto it = projNameMap.find(projectionName);
                                      if (it == projNameMap.end())
                                        projNameMap.emplace(projectionName, projectionList[0]);
                                    }
                                  }
                                } else if (headName == "By") {
                                  std::visit(
                                          [](auto &&argument) {
                                              return bossExprToVeloxBy(argument);
                                          },
                                          argument.getArgument());
                                } else if (headName == "Sum") {
                                  auto aName = fmt::format("a{}", aggrNameMap.size());
                                  aggrPair aggregation;
                                  aggregation.op = "sum";
                                  aggregation.oldName = tmpArhStr;
                                  aggregation.newName = aName;
                                  curVeloxExpr.aggregatesVec.push_back(aggregation);
                                  // new implicit name for maybe later orderBy
                                  aggrNameMap.emplace(tmpArhStr, aName);
                                  curVeloxExpr.orderBy = true;
                                  if (std::find(curVeloxExpr.selectedColumns.begin(),
                                                curVeloxExpr.selectedColumns.end(),
                                                tmpArhStr) == curVeloxExpr.selectedColumns.end())
                                    curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                                } else {
                                  std::visit([](auto &&argument) { return bossExprToVelox(argument); },
                                             argument.getArgument());
                                }
                              }
                          };
                          process(expression.getArguments());
                      },
                      [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
              (Expression::SuperType const &) expression);
    }

    boss::Expression evaluate_tpch(Expression const &e) {
      veloxExprList.clear();
      curVeloxExpr.clear();
      projNameMap.clear();
      aggrNameMap.clear();
      bossExprToVelox(e);
      veloxExprList.push_back(curVeloxExpr); //push back the last expression to the vector
      ExpressionArguments bossResults;

      const std::vector<int64_t> int64Vector = {1, 2, 3, 4, 5};
      const std::vector<int64_t> doubleVector = {10, 20, 30, 40, 50};

      arrow::Int64Builder a0_builder(arrow::default_memory_pool());
      arrow::Int64Builder a1_builder(arrow::default_memory_pool());
      a0_builder.AppendValues(int64Vector.begin(), int64Vector.end());
      a1_builder.AppendValues(doubleVector.begin(), doubleVector.end());
      std::shared_ptr<arrow::Array> arrayPtr0, arrayPtr1;
      a0_builder.Finish(&arrayPtr0);
      a1_builder.Finish(&arrayPtr1);

      auto vec0 = toVelox(*arrayPtr0);
      auto vec1 = toVelox(*arrayPtr1);
      auto veloxData = benchmark.makeRowVectorWrap({vec0, vec1});

      if (curVeloxExpr.tableName != "tmp") {
        auto columnAliaseList = queryBuilder->getFileColumnNamesMap(veloxExprList);
        queryBuilder->reformVeloxExpr(veloxExprList, columnAliaseList);
        const auto queryPlan = queryBuilder->getVeloxPlanBuilder(veloxExprList, columnAliaseList);
        const auto [cursor, results] = benchmark.run(queryPlan);
        auto task = cursor->task();
        ensureTaskCompletion(task.get());
#ifdef DebugInfo
        printResults(results);
        std::cout << std::endl;
#endif
        for (const auto &vector: results) {
          for (vector_size_t i = 0; i < vector->size(); ++i) {
            bossResults.emplace_back(vector->toString(i));
          }
        }
      } else {
        const auto queryPlan = queryBuilder->getVeloxPlanBuilderVector(veloxExprList, veloxData);
        auto [task, results] = bossQuery(queryPlan.plan);
#ifdef DebugInfo
        printResults(results);
        std::cout << std::endl;
#endif
        auto args = boss::expressions::ExpressionArguments();
        for (const auto &vector: results) {
          for (int i = 0; i < vector->childrenSize(); ++i) {
            auto array = toArrow(vector->childAt(i));
            auto &columnArray = static_cast<const arrow::Int64Array &>(*array);
            bossResults.clear();
            for (int j = 0; j < columnArray.length(); ++j) {
              bossResults.emplace_back(columnArray.Value(j));
            }
            args.push_back(boss::ComplexExpression(boss::Symbol("List"), std::move(bossResults)));
          }
        }
        return ComplexExpression("List"_, std::move(args));
      }

      auto result = boss::ComplexExpression(boss::Symbol("List"), move(bossResults));
      return result;
    }

//    boss::Expression evaluate(boss::Expression&& e) {
//      static unordered_map<boss::Symbol, StatelessOperator> operators;
//
//      operators["Select"_] = [](boss::ComplexExpression &&inputExpr) -> Expression {
//          return std::visit(boss::utilities::overload(
//                                    [](boss::ComplexExpression &&input) -> Expression {
//                                        boss::expressions::ExpressionSpanArguments results;
//                                        auto thing = std::get<ComplexExpression>(transformDynamicsToSpans(
//                                                std::move(std::move(input).getDynamicArguments().at(1))));
//                                        for (auto &span: thing.getSpanArguments()) {
//                                          arrow::Int64Builder a_builder(arrow::default_memory_pool());
//                                          a_builder.AppendValues(get < boss::Span<long long>>
//                                          (span).begin(), get < boss::Span<long long>>
//                                          (span).size());
//                                          std::shared_ptr<arrow::Array> arrayPtr;
//                                          a_builder.Finish(&arrayPtr);
//                                          auto vec = toVelox(*arrayPtr);
//                                          auto veloxData = benchmark.makeRowVectorWrap({vec});
//
//                                          const auto queryPlan = queryBuilder->getVeloxPlanBuilderVector(veloxExprList, veloxData);
//                                          auto [task, result] = bossQuery(queryPlan.plan);
//
//                                          auto array = toArrow(result[0]);
//                                          auto &columnArray = static_cast<const arrow::Int64Array&>(*array);
//                                          results.push_back(boss::Span<long long>((long long *)columnArray.raw_values(),columnArray.length(),  [](auto&& /*unused*/) {}));
//                                        }
//                                        auto args = boss::expressions::ExpressionArguments();
//                                        args.push_back(ComplexExpression("List"_, {}, {}, std::move(results)));
//                                        return ComplexExpression("List"_, std::move(args));
//                                    },
//                                    [](auto &&) -> Expression {
//                                        __builtin_debugtrap();
//                                        return "unknown"_();
//                                    }),
//                            evaluate(std::move(inputExpr).getArgument(0)));
//      };
//      operators["ScanColumns"_] = [](boss::Expression&& input) -> Expression {
//          return std::visit(
//                  [](auto&& val) -> Expression {
//                      if constexpr(std::is_same_v<std::decay_t<decltype(val)>, ComplexExpression>) {
//                        return std::move(val).getArgument(0);
//                      }
//                      return val;
//                  },
//                  std::move(input));
//      };
//
//      return visit(boss::utilities::overload(
//                           [](boss::ComplexExpression&& e) -> boss::Expression {
//                               auto head = e.getHead();
//                               if(operators.count(head))
//                                 return operators.at(head)(std::move(e));
//                               return std::move(e);
//                           },
//                           [](auto&& e) -> boss::Expression { return std::forward<decltype(e)>(e); }),
//                   std::move(e));
//    }

    Engine::Engine() {
      benchmark.initialize();
      queryBuilder = std::make_shared<BossQueryBuilder>(toFileFormat("parquet"));
      auto data_path = "../../velox_test_ZUpx2q";
      queryBuilder->initialize(data_path);
    }

    boss::Expression Engine::evaluate(boss::Expression&& e) {
      return evaluate_tpch(e.clone());
    }

} // namespace boss::engines::velox

static auto &enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::velox::Engine());
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
