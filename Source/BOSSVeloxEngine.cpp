#include "BOSSQueryBuilder.h"
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
//
using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

//#include "spdlog/cfg/env.h"
//#include "spdlog/sinks/basic_file_sink.h"
#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <algorithm>
std::ostream& operator<<(std::ostream& s, std::vector<std::int64_t> const& input /*unused*/) {
  std::for_each(begin(input), prev(end(input)),
                [&s = s << "["](auto&& element) { s << element << ", "; });
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

#define STRINGIFY(x) #x        // NOLINT
#define STRING(x) STRINGIFY(x) // NOLINT

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}

void printResults(const std::vector<RowVectorPtr>& results) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for (const auto& vector : results) {
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

class BossBenchmark {
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

  std::pair<std::unique_ptr<TaskCursor>, std::vector<RowVectorPtr>> run(const BossPlan& bossPlan) {
    CursorParameters params;
    params.maxDrivers = 4;
    params.planNode = bossPlan.plan;
    const int numSplitsPerFile = 10;

    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if(!noMoreSplits) {
        for(const auto& entry : bossPlan.dataFiles) {
          for(const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, numSplitsPerFile, bossPlan.dataFileFormat);
            for(const auto& split : splits) {
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

class Engine : public boss::Engine {
private:
  class EngineImplementation& impl;
  friend class EngineImplementation;

public:
  Engine(Engine&) = delete;
  Engine& operator=(Engine&) = delete;
  Engine(Engine&&) = default;
  Engine& operator=(Engine&&) = delete;
  Engine();
  boss::Expression evaluate(Expression const& e);
  ~Engine();
};
} // namespace boss::engines::velox

namespace nasty = boss::utilities::nasty;
namespace boss::engines::velox {
using ExpressionBuilder = boss::utilities::ExtensibleExpressionBuilder<VeloxExpressionSystem>;
static ExpressionBuilder operator""_(const char* name, size_t /*unused*/) {
  return ExpressionBuilder(name);
};

using std::set;
using std::string;
using std::to_string;
using std::vector;
using std::string_literals::operator""s;
using std::endl;

struct EngineImplementation {
  constexpr static char const* const DefaultNamespace = "BOSS`";
  BossBenchmark benchmark;
  std::shared_ptr<BossQueryBuilder> queryBuilder;
  std::vector<FormExpr> veloxExprList;
  FormExpr curVeloxExpr;

  static char const* removeNamespace(char const* symbolName) {
    if(strncmp(DefaultNamespace, symbolName, strlen(DefaultNamespace)) == 0) {
      return symbolName + strlen(DefaultNamespace);
    }
    return symbolName;
  }

  static auto mangle(std::string normalizedName) {
    normalizedName = std::regex_replace(normalizedName, std::regex("_"), "$$0");
    normalizedName = std::regex_replace(normalizedName, std::regex("\\."), "$$1");
    return normalizedName;
  }

  static auto demangle(std::string normalizedName) {
    normalizedName = std::regex_replace(normalizedName, std::regex("$0"), "_");
    normalizedName = std::regex_replace(normalizedName, std::regex("$1"), ".");
    return normalizedName;
  }

  static bool cmpFunCheck(std::string input) {
    static std::vector<std::string> cmpFun{"Greater", "Equal"};
    for(int i = 0; i < cmpFun.size(); i++) {
      if(input == cmpFun[i]) {
        return 1;
      }
    }
    return 0;
  }

  void mergeGreaterFilter(FiledFilter input) {
    if(input.element[0].type == cName) {
      auto colName = input.element[0].data;
      for(int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
        if(curVeloxExpr.tmpFieldFiltersVec[i].element[1].type == cName &&
           curVeloxExpr.tmpFieldFiltersVec[i].element[1].data == colName) {
          input.element.push_back(curVeloxExpr.tmpFieldFiltersVec[i].element[0]);
          input.opName = "Between";
          curVeloxExpr.tmpFieldFiltersVec[i] = input;
          return;
        }
      }
      curVeloxExpr.tmpFieldFiltersVec.push_back(input);
    } else if(input.element[1].type == cName) {
      auto colName = input.element[1].data;
      for(int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
        if(curVeloxExpr.tmpFieldFiltersVec[i].element[0].type == cName &&
           curVeloxExpr.tmpFieldFiltersVec[i].element[0].data == colName) {
          curVeloxExpr.tmpFieldFiltersVec[i].opName = "Between";
          curVeloxExpr.tmpFieldFiltersVec[i].element.push_back(input.element[0]);
          return;
        }
      }
      curVeloxExpr.tmpFieldFiltersVec.push_back(input);
    }
  }

  void formatVeloxFilter() {
    std::string tmp;
    for(int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
      if(curVeloxExpr.tmpFieldFiltersVec[i].opName == "Greater") {
        if(curVeloxExpr.tmpFieldFiltersVec[i].element[0].type ==
           cName) // the column name should be on the left side.
          tmp = fmt::format("{} > {}", curVeloxExpr.tmpFieldFiltersVec[i].element[0].data,
                            curVeloxExpr.tmpFieldFiltersVec[i].element[1].data);
        else
          tmp = fmt::format("{} < {}", curVeloxExpr.tmpFieldFiltersVec[i].element[1].data,
                            curVeloxExpr.tmpFieldFiltersVec[i].element[0].data);

      } else if(curVeloxExpr.tmpFieldFiltersVec[i].opName == "Between") {
        tmp =
            fmt::format("{} between {} and {}", curVeloxExpr.tmpFieldFiltersVec[i].element[0].data,
                        curVeloxExpr.tmpFieldFiltersVec[i].element[1].data,
                        curVeloxExpr.tmpFieldFiltersVec[i].element[2].data);
      } else if(curVeloxExpr.tmpFieldFiltersVec[i].opName == "Equal") {
        tmp = fmt::format("{} = {}", curVeloxExpr.tmpFieldFiltersVec[i].element[0].data,
                          curVeloxExpr.tmpFieldFiltersVec[i].element[1].data);
      } else
        VELOX_FAIL("unexpected Filter type");
      curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
    }
  }

  static void formatVeloxProjection(std::vector<std::string>& projectionList) {
    std::vector<std::string> input(3);
    std::string out;
    for(int i = 0; i < 3; i++) {
      auto tmp = projectionList.back();
      input[2 - i] = tmp;
      projectionList.pop_back();
    }

    if(input[0] == "Multiply" || input[0] == "Times") {
      out = fmt::format("({}) * ({})", input[1], input[2]);
    } else if(input[0] == "Minus" || input[0] == "Subtract") {
      out = fmt::format("({}) - ({})", input[1], input[2]);
    } else
      VELOX_FAIL("unexpected Projection type");
    projectionList.push_back(out);
  }

  void bossExprToVeloxFilter(Expression const& expression) {
    static FiledFilter tmpFieldFilter;
    std::visit(
        boss::utilities::overload(
            [&](bool a) {
              AtomicExpr element;
              element.data = to_string(a);
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](std::vector<bool>::reference a) {
            },
            [&](std::int64_t a) {
              AtomicExpr element;
              element.data = to_string(a);
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](std::vector<int64_t> values) {
              ExpressionSpanArguments vs;
              vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
              bossExprToVeloxFilter(ComplexExpression("List"_, {}, {}, std::move(vs)));
            },
            [&](char const* a) {
              AtomicExpr element;
              element.data = a;
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](std::double_t a) {
              AtomicExpr element;
              element.data = to_string(a);
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](Symbol const& a) {
              if(std::find(curVeloxExpr.selectedColumns.begin(),
                           curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                           a.getName()) == curVeloxExpr.selectedColumns.end())
                curVeloxExpr.selectedColumns.push_back(a.getName());
              AtomicExpr element;
              element.data = a.getName();
              element.type = cName;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](std::string const& a) {
              AtomicExpr element;
              element.data = a;
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](ComplexExpression const& expression) {
              auto headName = expression.getHead().getName();
              std::cout << "headName  " << headName << endl;
              if(cmpFunCheck(headName)) { // field filter or join op pre-process
                tmpFieldFilter.opName.clear();
                tmpFieldFilter.element.clear();
                tmpFieldFilter.opName = headName;
              }
              auto process = [&](auto const& arguments) {
                for(auto it = arguments.begin(); it != arguments.end(); ++it) {
                  auto const& argument = *it;
                  std::cout << "argument  " << argument << endl;
                  std::stringstream out;
                  out << argument;
                  std::string tmpArhStr = out.str();

                  if(tmpArhStr.substr(0, 10) == "DateObject") {
                    std::string dateString = tmpArhStr.substr(12, 10);
                    std::cout << "DateObject  " << dateString << endl;
                    AtomicExpr element;
                    element.data = "'" + dateString + "'" + "::DATE";
                    element.type = cValue;
                    tmpFieldFilter.element.emplace_back(element);
                    continue;
                  }
                  if(tmpArhStr.substr(0, 4) == "Date") {
                    std::string dateString = tmpArhStr.substr(6, 10);
                    std::cout << "Date  " << dateString << endl;
                    AtomicExpr element;
                    element.data = "'" + dateString + "'" + "::DATE";
                    element.type = cValue;
                    tmpFieldFilter.element.emplace_back(element);
                    continue;
                  }

                  if constexpr(std::is_same_v<std::decay_t<decltype(argument)>, Expression>) {
                    bossExprToVeloxFilter(argument);
                  } else {
                    std::visit([this](auto&& argument) { return bossExprToVeloxFilter(argument); },
                               argument.getArgument());
                  }
                }
                if(!tmpFieldFilter.opName.empty()) { // field filter or join op post-process
                  if(headName == "Greater")
                    mergeGreaterFilter(tmpFieldFilter);
                }
              };
              process(expression.getArguments());
            },
            [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
        (Expression::SuperType const&)expression);
  }

  void bossExprToVeloxProjection(Expression const& expression, std::vector<std::string>& projectionList) {
    std::visit(
        boss::utilities::overload(
            [&](bool a) {
              projectionList.push_back(to_string(a));
            },
            [&](std::vector<bool>::reference a) {
            },
            [&](std::int64_t a) {
              projectionList.push_back(to_string(a));
            },
            [&](std::vector<int64_t> values) {
              ExpressionSpanArguments vs;
              vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
              bossExprToVeloxProjection(ComplexExpression("List"_, {}, {}, std::move(vs)),
                                        projectionList);
            },
            [&](char const* a) {
              projectionList.push_back(a);
            },
            [&](std::double_t a) {
              projectionList.push_back(to_string(a));
            },
            [&](Symbol const& a) {
              if(std::find(curVeloxExpr.selectedColumns.begin(),
                           curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                           a.getName()) == curVeloxExpr.selectedColumns.end())
                curVeloxExpr.selectedColumns.push_back(a.getName());
              projectionList.push_back(a.getName());
            },
            [&](std::string const& a) {
              projectionList.push_back(a);
            },
            [&](ComplexExpression const& expression) {
              auto headName = expression.getHead().getName();
              std::cout << "headName  " << headName << endl;
              projectionList.push_back(headName);
              auto process = [&](auto const& arguments) {
                for(auto it = arguments.begin(); it != arguments.end(); ++it) {
                  auto const& argument = *it;
                  std::cout << "argument  " << argument << endl;
                  std::stringstream out;
                  out << argument;
                  std::string tmpArhStr = out.str();

                  if constexpr(std::is_same_v<std::decay_t<decltype(argument)>, Expression>) {
                    bossExprToVeloxProjection(argument, projectionList);
                  } else {
                    std::visit(
                        [this, &projectionList](auto&& argument) {
                          return bossExprToVeloxProjection(argument, projectionList);
                        },
                        argument.getArgument());
                  }
                }
                if(projectionList.size() >= 3) { // field filter or join op post-process
                  formatVeloxProjection(projectionList);
                }
              };
              process(expression.getArguments());
            },
            [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
        (Expression::SuperType const&)expression);
  }

  void bossExprToVeloxlPartialAggregation(Expression const& expression, std::vector<std::string>& aggregationList) {
    std::visit(
        boss::utilities::overload(
            [&](char const* a) {
              aggregationList.push_back(a);
            },
            [&](Symbol const& a) {
              aggregationList.push_back(a.getName());
            },
            [&](std::string const& a) {
              aggregationList.push_back(a);
            },
            [&](ComplexExpression const& expression) {
              auto headName = expression.getHead().getName();
              std::cout << "headName  " << headName << endl;
              auto process = [&](auto const& arguments) {
                for(auto it = arguments.begin(); it != arguments.end(); ++it) {
                  auto const& argument = *it;
                  std::cout << "argument  " << argument << endl;
                  std::stringstream out;
                  out << argument;
                  std::string tmpArhStr = out.str();
                  std::visit(
                      [this, &aggregationList](auto&& argument) {
                        return bossExprToVeloxlPartialAggregation(argument, aggregationList);
                      },
                      argument.getArgument());
                }
              };
              process(expression.getArguments());
            },
            [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
        (Expression::SuperType const&)expression);
  }

  void bossExprToVelox(Expression const& expression) {
    std::visit(
        boss::utilities::overload(
            [&](bool a) {
            },
            [&](std::vector<bool>::reference a) {
            },
            [&](std::int64_t a) {
            },
            [&](std::vector<int64_t> values) {
              ExpressionSpanArguments vs;
              vs.emplace_back(Span<std::int64_t>({values.begin(), values.end()}));
              bossExprToVelox(ComplexExpression("List"_, {}, {}, std::move(vs)));
            },
            [&](char const* a) {
            },
            [&](std::double_t a) {
            },
            [&](Symbol const& a) {
              if(std::find(curVeloxExpr.selectedColumns.begin(),
                           curVeloxExpr.selectedColumns.end(), // avoid repeated selectedColumns
                           a.getName()) == curVeloxExpr.selectedColumns.end())
                curVeloxExpr.selectedColumns.push_back(a.getName());
            },
            [&](std::string const& a) {
            },
            [&](ComplexExpression const& expression) {
              std::string projectionName;
              auto headName = expression.getHead().getName();
              std::cout << "headName  " << headName << endl;
              auto process = [&](auto const& arguments) {
                for(auto it = arguments.begin(); it != arguments.end(); ++it) {
                  auto const& argument = *it;
                  std::cout << "argument  " << argument << endl;
                  std::stringstream out;
                  out << argument;
                  std::string tmpArhStr = out.str();

                  if constexpr(std::is_same_v<std::decay_t<decltype(argument)>, Expression>) {
                    bossExprToVelox(argument);
                  } else if(headName == "Where") {
                    std::visit([this](auto&& argument) { return bossExprToVeloxFilter(argument); },
                               argument.getArgument());
                    formatVeloxFilter();
                  } else if(headName == "Select" && it == arguments.begin()) {
                    if(!curVeloxExpr.tableName.empty()) // traverse for a new plan builder
                    {
                      veloxExprList.push_back(curVeloxExpr);
                      curVeloxExpr.clear();
                    }
                    curVeloxExpr.tableName = tmpArhStr; // indicate table names
                  } else if(headName == "As") {
                    if(it == arguments.begin()) {
                      projectionName = tmpArhStr;
                    } else {
                      std::vector<std::string> projectionList;
                      std::visit(
                          [this, &projectionList](auto&& argument) {
                            return bossExprToVeloxProjection(argument, projectionList);
                          },
                          argument.getArgument());
                      auto projection = fmt::format("{} AS {}", projectionList[0], projectionName);
                      curVeloxExpr.projectionsVec.push_back(projection);
                    }
                  } else if(headName == "By") { // for PartialAggregation and groupby both
                    std::vector<std::string> aggregationList;
                    std::visit(
                        [this, &aggregationList](auto&& argument) {
                          return bossExprToVeloxlPartialAggregation(argument, aggregationList);
                        },
                        argument.getArgument());
                    curVeloxExpr.groupingKeysVec = std::move(aggregationList);
                  } else if(headName == "Sum") {
                    auto aggregation = fmt::format("sum({})", tmpArhStr);
                    curVeloxExpr.aggregatesVec.push_back(aggregation);
                  } else {
                    std::visit([this](auto&& argument) { return bossExprToVelox(argument); },
                               argument.getArgument());
                  }
                }
              };
              process(expression.getArguments());
            },
            [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
        (Expression::SuperType const&)expression);
  }

  static ExpressionBuilder namespaced(ExpressionBuilder const& builder) {
    return ExpressionBuilder(Symbol(DefaultNamespace + Symbol(builder).getName()));
  }
  static Symbol namespaced(Symbol const& name) {
    return move(ExpressionBuilder(Symbol(DefaultNamespace + name.getName())));
  }
  static ComplexExpression namespaced(ComplexExpression&& name) {
    return move(ComplexExpression(Symbol(DefaultNamespace + name.getHead().getName()),
                                  move(name.getArguments())));
  }

  void evalWithoutNamespace(Expression const& expression) { evaluate(expression, ""); };

  EngineImplementation() {}

  EngineImplementation(EngineImplementation&&) = default;
  EngineImplementation(EngineImplementation const&) = delete;
  EngineImplementation& operator=(EngineImplementation&&) = delete;
  EngineImplementation& operator=(EngineImplementation const&) = delete;

  ~EngineImplementation() {}

  boss::Expression evaluate(Expression const& e,
                            std::string const& namespaceIdentifier = DefaultNamespace) {
    std::cout << e << std::endl;
    veloxExprList.clear();
    curVeloxExpr.clear();
    bossExprToVelox(e);
    veloxExprList.push_back(curVeloxExpr);
    const auto queryPlan = queryBuilder->getVeloxPlanBuilder(veloxExprList);
    const auto [cursor, actualResults] = benchmark.run(queryPlan);
    auto task = cursor->task();
    ensureTaskCompletion(task.get());

    printResults(actualResults);
    std::cout << std::endl;

    //    const auto stats = task->taskStats();
    //    std::cout << fmt::format("Execution time: {}",
    //                             succinctMillis(stats.executionEndTimeMs -
    //                             stats.executionStartTimeMs))
    //              << std::endl;
    //    std::cout << fmt::format("Splits total: {}, finished: {}", stats.numTotalSplits,
    //                             stats.numFinishedSplits)
    //              << std::endl;
    //    std::cout << printPlanWithStats(*queryPlan.plan, stats, false) << std::endl;
    //    std::cout << std::endl;
    return true;
  }
};

Engine::Engine() : impl([]() -> EngineImplementation& { return *(new EngineImplementation()); }()) {
  impl.benchmark.initialize();
  impl.queryBuilder = std::make_shared<BossQueryBuilder>(toFileFormat("parquet"));
  auto data_path = "/Users/sunny/Documents/tmp/velox_test_ZUpx2q";
  impl.queryBuilder->initialize(data_path);
}
Engine::~Engine() { delete &impl; }

boss::Expression Engine::evaluate(Expression const& e) { return impl.evaluate(e); }
} // namespace boss::engines::velox

#ifdef _WIN32
#define BOSS_VELOX_API __declspec(dllexport)
#else
#define BOSS_VELOX_API
#endif // _WIN32

static auto& enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::velox::Engine>();
  if(!engine && initialise) {
    engine.reset(new boss::engines::velox::Engine());
  }
  return engine;
}

extern "C" BOSS_VELOX_API BOSSExpression* evaluate(BOSSExpression* e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto* r = new BOSSExpression{enginePtr()->evaluate(e->delegate.clone())};
  return r;
};

extern "C" BOSS_VELOX_API void reset() { enginePtr(false).reset(nullptr); }

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved) {
  switch(fdwReason) {
  case DLL_PROCESS_ATTACH:
  case DLL_THREAD_ATTACH:
  case DLL_THREAD_DETACH:
    break;
  case DLL_PROCESS_DETACH:
    // Make sure to call reset instead of letting destructors to be called.
    // It leaves the engine unique_ptr in a non-dangling state
    // in case the depending process still want to call reset() during its own destruction
    // (which does happen in a unpredictable order if it is itself a dll:
    // https://devblogs.microsoft.com/oldnewthing/20050523-05/?p=35573)
    reset();
    break;
  }
  return TRUE;
}
#endif
