#include "BOSSQueryBuilder.h"

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
//#include "velox/exec/PlanNodeStats.h"
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
//#define DebugInfo

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
  std::unordered_map<std::string, std::string> projNameMap;
  std::unordered_map<std::string, std::string> aggrNameMap;

  static bool cmpFunCheck(std::string input) {
    static std::vector<std::string> cmpFun{"Greater", "Equal", "StringContains"};
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

  void bossExprToVeloxFilter_Join(Expression const& expression) {
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
            [&](char const* a) {
              AtomicExpr element;
              element.data = a;
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
              element.data = "'" + a + "'";
              element.type = cValue;
              tmpFieldFilter.element.emplace_back(element);
            },
            [&](ComplexExpression const& expression) {
              auto headName = expression.getHead().getName();
#ifdef DebugInfo
              std::cout << "headName  " << headName << endl;
#endif
              if (cmpFunCheck(headName)) { // field filter or join op pre-process
                tmpFieldFilter.clear();
                tmpFieldFilter.opName = headName;
              }
              auto process = [&](auto const& arguments) {
                for(auto it = arguments.begin(); it != arguments.end(); ++it) {
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

                  std::visit([this](auto &&argument) { return bossExprToVeloxFilter_Join(argument); },
                             argument.getArgument());
                }
                if (!tmpFieldFilter.opName.empty()) { // field filter or join op post-process
                  if (headName == "Greater")
                    mergeGreaterFilter(tmpFieldFilter);
                  else if ((headName == "Equal" || headName == "StringContains") && tmpFieldFilter.element.size() > 0) {
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
        (Expression::SuperType const&)expression);
  }

  void bossExprToVeloxProj_PartialAggr(Expression const& expression, std::vector<std::string>& projectionList) {
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
            [&](char const* a) {
              projectionList.push_back(a);
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
#ifdef DebugInfo
              std::cout << "headName  " << headName << endl;
#endif
              projectionList.push_back(headName);
              auto process = [&](auto const& arguments) {
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
                    if (std::find(curVeloxExpr.selectedColumns.begin(), curVeloxExpr.selectedColumns.end(),
                                  tmpArhStr) == curVeloxExpr.selectedColumns.end())
                      curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                    continue;
                  }

                  std::visit(
                          [this, &projectionList](auto &&argument) {
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
        (Expression::SuperType const&)expression);
  }

  void bossExprToVeloxBy(Expression const& expression) {
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
            [&](std::string const& a) {
            },
            [&](ComplexExpression const& expression) {
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
                            [this](auto &&argument) {
                                return bossExprToVeloxBy(argument);
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
                curVeloxExpr.limit = a;
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
              if (!curVeloxExpr.tableName.empty()) // traverse for a new plan builder, triggerred by select and join
              {
                veloxExprList.push_back(curVeloxExpr);
                curVeloxExpr.clear();
              }
              curVeloxExpr.tableName = a.getName(); // indicate table names
            },
            [&](std::string const& a) {
            },
            [&](ComplexExpression const& expression) {
                std::string projectionName;
                auto headName = expression.getHead().getName();
#ifdef DebugInfo
                std::cout << "headName  " << headName << endl;
#endif
                auto process = [&](auto const &arguments) {
                    std::vector<std::string> lastProjectionsVec;
                    if (headName == "As") { //save the latest ProjectionsVec to restore for AS - aggregation
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
                        std::visit([this](auto &&argument) { return bossExprToVeloxFilter_Join(argument); },
                                   argument.getArgument());
                        if (curVeloxExpr.tmpFieldFiltersVec.size() > 0)
                          formatVeloxFilter_Join();
                      } else if (headName == "As") {
                        if ((it - arguments.begin()) % 2 == 0) {
                          projectionName = tmpArhStr;
                        } else {
                          std::vector<std::string> projectionList;
                          std::visit(
                                  [this, &projectionList](auto &&argument) {
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
                                [this](auto &&argument) {
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
                        if (std::find(curVeloxExpr.selectedColumns.begin(), curVeloxExpr.selectedColumns.end(),
                                      tmpArhStr) == curVeloxExpr.selectedColumns.end())
                          curVeloxExpr.selectedColumns.push_back(tmpArhStr);
                      } else {
                        std::visit([this](auto &&argument) { return bossExprToVelox(argument); },
                                   argument.getArgument());
                      }
                    }
                };
                process(expression.getArguments());
            },
            [](auto /*args*/) { throw std::runtime_error("unexpected argument type"); }),
        (Expression::SuperType const&)expression);
  }

  EngineImplementation() {}

  EngineImplementation(EngineImplementation&&) = default;
  EngineImplementation(EngineImplementation const&) = delete;
  EngineImplementation& operator=(EngineImplementation&&) = delete;
  EngineImplementation& operator=(EngineImplementation const&) = delete;

  ~EngineImplementation() {}

  boss::Expression evaluate(Expression const& e,
                            std::string const& namespaceIdentifier = DefaultNamespace) {
    veloxExprList.clear();
    curVeloxExpr.clear();
    projNameMap.clear();
    aggrNameMap.clear();
    bossExprToVelox(e);
    veloxExprList.push_back(curVeloxExpr); //push back the last expression to the vector
    auto columnAliaseList = queryBuilder->getFileColumnNamesMap(veloxExprList);
    queryBuilder->reformVeloxExpr(veloxExprList, columnAliaseList);
    const auto queryPlan = queryBuilder->getVeloxPlanBuilder(veloxExprList, columnAliaseList);
    const auto [cursor, actualResults] = benchmark.run(queryPlan);
    auto task = cursor->task();
    ensureTaskCompletion(task.get());
#ifdef DebugInfo
    printResults(actualResults);
    std::cout << std::endl;
//    const auto stats = task->taskStats();
//    std::cout << fmt::format("Execution time: {}",
//                             succinctMillis(stats.executionEndTimeMs -
//                                            stats.executionStartTimeMs))
//              << std::endl;
//    std::cout << fmt::format("Splits total: {}, finished: {}", stats.numTotalSplits,
//                             stats.numFinishedSplits)
//              << std::endl;
//    std::cout << printPlanWithStats(*queryPlan.plan, stats, false) << std::endl;
#endif
    std::cout << std::endl;
    ExpressionArguments bossResults;
    for (const auto &vector: actualResults) {
      for (vector_size_t i = 0; i < vector->size(); ++i) {
        bossResults.emplace_back(vector->toString(i));
      }
    }
    auto result = boss::ComplexExpression(boss::Symbol("List"), move(bossResults));
    return result;
  }
};

Engine::Engine() : impl([]() -> EngineImplementation& { return *(new EngineImplementation()); }()) {
  impl.benchmark.initialize();
  impl.queryBuilder = std::make_shared<BossQueryBuilder>(toFileFormat("parquet"));
  auto data_path = "../../velox_test_ZUpx2q";
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
