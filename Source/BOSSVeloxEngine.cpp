#include "BOSSVeloxEngine.hpp"

#include "BOSSCoreTmp.h"
#include "BridgeVelox.h"

#include "velox/exec/PlanNodeStats.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"

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
#include <regex>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

#ifdef DebugInfo
#include <iostream>
#endif // DebugInfo

using std::get;
using std::is_invocable_v;
using std::move;
using std::to_string;
using std::unordered_map;
using std::string_literals::operator""s;

using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;
using boss::Span;
using boss::Symbol;
using boss::expressions::generic::isComplexExpression;

namespace boss {
using SpanInputs =
    std::variant<std::vector<std::int32_t>, std::vector<std::int64_t>, std::vector<std::float_t>,
                 std::vector<std::double_t>, std::vector<std::string>, std::vector<Symbol>>;
} // namespace boss

namespace boss::engines::velox {

using ComplexExpression = boss::expressions::ComplexExpression;
template <typename... T>
using ComplexExpressionWithStaticArguments =
    boss::expressions::ComplexExpressionWithStaticArguments<T...>;
using Expression = boss::expressions::Expression;
using ExpressionArguments = boss::expressions::ExpressionArguments;
using ExpressionSpanArguments = boss::expressions::ExpressionSpanArguments;
using ExpressionSpanArgument = boss::expressions::ExpressionSpanArgument;
using expressions::generic::ArgumentWrapper;
using expressions::generic::ExpressionArgumentsWithAdditionalCustomAtomsWrapper;

template <typename T> boss::Span<const T> createBossSpan(VectorPtr const& vec) {
  auto const* data = vec->values()->as<T>();
  auto length = vec->size();
  VectorPtr ptrCopy = vec;
  return boss::Span<const T>(data, length, [v = std::move(ptrCopy)]() {});
}

static ExpressionSpanArgument veloxtoSpan(VectorPtr const& vec) {
  if(vec->typeKind() == TypeKind::INTEGER) {
    return createBossSpan<int32_t>(vec);
  }
  if(vec->typeKind() == TypeKind::BIGINT) {
    return createBossSpan<int64_t>(vec);
  }
  if(vec->typeKind() == TypeKind::REAL) {
    return createBossSpan<float_t>(vec);
  }
  if(vec->typeKind() == TypeKind::DOUBLE) {
    return createBossSpan<double_t>(vec);
  }
  throw std::runtime_error("veloxToSpan: array type not supported: " +
                           facebook::velox::mapTypeKindToName(vec->typeKind()));
}

template <typename T>
VectorPtr spanToVelox(boss::Span<T>&& span, memory::MemoryPool* pool, BufferPtr indices = nullptr) {
  BossArray bossArray(span.size(), span.begin(), std::move(span));

  auto createDictVector = [](BufferPtr& indices, auto flatVecPtr) {
    auto indicesSize = indices->size() / sizeof(int32_t);
    return BaseVector::wrapInDictionary(BufferPtr(nullptr), indices, indicesSize,
                                        std::move(flatVecPtr));
  };

  auto createVeloxVector = [&](auto bossType) {
    auto flatVecPtr = importFromBossAsOwner(bossType, bossArray, pool);
    if(indices == nullptr) {
      return flatVecPtr;
    }
    return createDictVector(indices, flatVecPtr);
  };

  if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int32_t const>) {
    return createVeloxVector(BossType::bINTEGER);
  } else if constexpr(std::is_same_v<T, int64_t> || std::is_same_v<T, int64_t const>) {
    return createVeloxVector(BossType::bBIGINT);
  } else if constexpr(std::is_same_v<T, float_t> || std::is_same_v<T, float_t const>) {
    return createVeloxVector(BossType::bREAL);
  } else if constexpr(std::is_same_v<T, double_t> || std::is_same_v<T, double_t const>) {
    return createVeloxVector(BossType::bDOUBLE);
  }
}

static int32_t dateToInt32(const std::string& str) {
  std::istringstream iss;
  iss.str(str);
  struct std::tm tm = {};
  iss >> std::get_time(&tm, "%Y-%m-%d");
  auto t = std::mktime(&tm);
  return (int32_t)std::chrono::duration_cast<std::chrono::days>(
             std::chrono::system_clock::from_time_t(t).time_since_epoch())
      .count();
}

static Expression dateProcess(Expression&& e) {
  return visit(boss::utilities::overload(
                   [](ComplexExpression&& e) -> Expression {
                     auto head = e.getHead();
                     if(head.getName() == "DateObject") {
                       auto argument = e.getArguments().at(0);
                       std::stringstream out;
                       out << argument;
                       auto dateString = out.str().substr(1, 10);
                       return dateToInt32(dateString);
                     }
                     // at least evaluate all the arguments
                     auto [_, statics, dynamics, spans] = std::move(e).decompose();
                     std::transform(std::make_move_iterator(dynamics.begin()),
                                    std::make_move_iterator(dynamics.end()), dynamics.begin(),
                                    [](auto&& arg) { return dateProcess(std::move(arg)); });
                     return ComplexExpression{std::move(head), std::move(statics),
                                              std::move(dynamics), std::move(spans)};
                   },
                   [](auto&& e) -> Expression { return std::forward<decltype(e)>(e); }),
               std::move(e));
}

template <typename... StaticArgumentTypes>
ComplexExpressionWithStaticArguments<StaticArgumentTypes...>
transformDynamicsToSpans(ComplexExpressionWithStaticArguments<StaticArgumentTypes...>&& input_) {
  std::vector<boss::SpanInputs> spanInputs;
  auto [head, statics, dynamics, oldSpans] = std::move(input_).decompose();

  auto it = std::move_iterator(dynamics.begin());
  for(; it != std::move_iterator(dynamics.end()); ++it) {
    if(!std::visit(
           [&spanInputs]<typename InputType>(InputType&& argument) {
             using Type = std::decay_t<InputType>;
             if constexpr(boss::utilities::isVariantMember<std::vector<Type>,
                                                           boss::SpanInputs>::value) {
               if(!spanInputs.empty() &&
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
      std::back_inserter(spans), [](auto&& untypedInput) {
        return std::visit(
            []<typename Element>(std::vector<Element>&& input) -> ExpressionSpanArgument {
              auto* ptr = input.data();
              auto size = input.size();
              spanReferenceCounter.add(ptr, [v = std::move(input)]() {});
              return boss::Span<Element>(ptr, size, [ptr]() { spanReferenceCounter.remove(ptr); });
            },
            std::forward<decltype(untypedInput)>(untypedInput));
      });

  std::copy(std::move_iterator(oldSpans.begin()), std::move_iterator(oldSpans.end()),
            std::back_inserter(spans));
  return {std::move(head), std::move(statics), std::move(dynamics), std::move(spans)};
}

static Expression transformDynamicsToSpans(Expression&& input) {
  return std::visit(
      [](auto&& x) -> Expression {
        if constexpr(std::is_same_v<std::decay_t<decltype(x)>, ComplexExpression>) {
          return transformDynamicsToSpans(std::forward<decltype(x)>(x));
        } else {
          return x;
        }
      },
      std::move(input));
}

static std::vector<BufferPtr> getIndices(ExpressionSpanArguments&& listSpans) {
  if(listSpans.empty()) {
    throw std::runtime_error("get index error");
  }

  std::vector<BufferPtr> indicesVec;
  for(auto& subSpan : listSpans) {
    BufferPtr indexData = std::visit(
        []<typename T>(boss::Span<T>&& typedSpan) -> BufferPtr {
          if constexpr(std::is_same_v<T, int32_t>) {
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

static std::tuple<std::vector<RowVectorPtr>, RowTypePtr, std::vector<size_t>>
getColumns(ComplexExpression&& expression, memory::MemoryPool* pool) {
  std::vector<RowVectorPtr> rowDataVec;
  std::vector<size_t> spanRowCountVec;
  std::vector<BufferPtr> indicesVec;
  auto indicesColumnEndIndex = size_t(-1);

  if(expression.getHead().getName() == "Gather") {
    auto [head, statics, dynamics, spans] = std::move(expression).decompose();
    auto it_start = std::move_iterator(dynamics.begin());
    indicesVec = getIndices(std::move(spans));
    expression = std::get<ComplexExpression>(std::move(*it_start));
  }

  if(expression.getHead().getName() == "RadixPartition") {
    auto [head, statics, dynamics, spans] = std::move(expression).decompose();
    auto it = std::move_iterator(dynamics.begin());
    auto it_end = std::move_iterator(dynamics.end());
    auto table = std::get<ComplexExpression>(std::move(*it++));
    auto [tableHead, tableStatic, tableDyn, tableSpans] = std::move(table).decompose();
    // add the positions for the additional columns (i.e. not the key columns)
    indicesColumnEndIndex = tableDyn.size();
    indicesVec = getIndices(std::move(spans));
    // add the key columns
    for(; it != it_end; ++it) {
      tableDyn.emplace_back(std::move(*it));
    }
    // get table data
    expression = boss::ComplexExpression(std::move(tableHead), std::move(tableStatic),
                                         std::move(tableDyn), std::move(tableSpans));
  }

  ExpressionArguments columns = std::move(expression).getArguments();
  std::vector<std::string> colNameVec;
  std::vector<std::shared_ptr<const Type>> colTypeVec;
  std::vector<std::vector<VectorPtr>> colDataListVec;

  std::for_each(
      std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
      [&colNameVec, &colTypeVec, &colDataListVec, pool, &indicesVec,
       &indicesColumnEndIndex](auto&& columnExpr) {
        auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
        auto [head, unused_, dynamics, spans] = std::move(column).decompose();

#ifdef USE_NEW_TABLE_FORMAT
        auto columnName = head.getName();
        auto dynamic = get<ComplexExpression>(std::move(dynamics.at(0)));
#else
        auto columnName = get<Symbol>(std::move(dynamics.at(0))).getName();
        auto dynamic = get<ComplexExpression>(std::move(dynamics.at(1)));
#endif
        std::transform(columnName.begin(), columnName.end(), columnName.begin(), ::tolower);

        auto list = transformDynamicsToSpans(std::move(dynamic));
        auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
        if(listSpans.empty()) {
          return;
        }

        int numSpan = 0;
        std::vector<VectorPtr> colDataVec;
        for(auto& subSpan : listSpans) {
          BufferPtr indices = nullptr;
          if(!indicesVec.empty() && colNameVec.size() < indicesColumnEndIndex) {
            indices = indicesVec[numSpan++];
          }
          VectorPtr subColData = std::visit(
              [pool, &indices, &columnName]<typename T>(boss::Span<T>&& typedSpan) -> VectorPtr {
                if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                             std::is_same_v<T, double_t> || std::is_same_v<T, int32_t const> ||
                             std::is_same_v<T, int64_t const> ||
                             std::is_same_v<T, double_t const>) {
                  return spanToVelox<T>(std::move(typedSpan), pool, indices);
                } else {
                  throw std::runtime_error(
                      "unsupported column type: '" + columnName +
                      "' with type: " + std::string(typeid(decltype(typedSpan)).name()));
                }
              },
              std::move(subSpan));
          colDataVec.push_back(std::move(subColData));
        }
        auto columnType = colDataVec[0]->type();
        colNameVec.emplace_back(columnName);
        colTypeVec.emplace_back(columnType);
        colDataListVec.push_back(std::move(colDataVec));
      });

  if(!colDataListVec.empty()) {
    auto listSize = colDataListVec[0].size();
    for(auto i = 0; i < listSize; i++) {
      spanRowCountVec.push_back(colDataListVec[0][i]->size());
      std::vector<VectorPtr> rowData;
      for(auto j = 0; j < columns.size(); j++) {
        assert(colDataListVec[j].size() == listSize);
        rowData.push_back(std::move(colDataListVec[j][i]));
      }
      auto rowVector = makeRowVectorNoCopy(colNameVec, rowData, pool);
      rowDataVec.push_back(std::move(rowVector));
    }
  }

  auto tableSchema =
      TypeFactory<TypeKind::ROW>::create(std::move(colNameVec), std::move(colTypeVec));
  return {rowDataVec, tableSchema, spanRowCountVec};
}

static std::string projectionExpressionToString(Expression&& e);
static std::string projectionExpressionToString(ComplexExpression&& e) {
  auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
  if(head.getName() == "Where") {
    auto it = std::make_move_iterator(dynamics.begin());
    return projectionExpressionToString(std::move(*it));
  }
  if(head.getName() == "Equal") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("{} = {}", lhs, rhs);
  }
  if(head.getName() == "StringContainsQ") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("{} like '%{}%'", lhs, rhs);
  }
  if(head.getName() == "DateObject") {
    auto it = std::make_move_iterator(dynamics.begin());
    return std::to_string(dateToInt32(projectionExpressionToString(std::move(*it++))));
  }
  if(head.getName() == "Year") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("cast(((cast({} AS DOUBLE) + 719563.285) / 365.265) AS INTEGER)",
                       projectionExpressionToString(std::move(*it++)));
  }
  if(head.getName() == "Greater") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} > {})", lhs, rhs);
  }
  if(head.getName() == "And") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} AND {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Plus") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} + {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Times" || head.getName() == "Multiply") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    return std::accumulate(std::next(it), itEnd, projectionExpressionToString(std::move(*it++)),
                           [](std::string&& cumul, auto&& arg) {
                             return fmt::format("({} * {})", cumul,
                                                projectionExpressionToString(std::move(arg)));
                           });
  }
  if(head.getName() == "Minus" || head.getName() == "Subtract") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} - {})", lhs, rhs);
  }
  if(head.getName() == "Divide") {
    auto it = std::make_move_iterator(dynamics.begin());
    auto lhs = projectionExpressionToString(std::move(*it++));
    auto rhs = projectionExpressionToString(std::move(*it++));
    return fmt::format("({} / {})", lhs, rhs);
  }
  if(head.getName() == "Count") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("count({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Sum") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("sum({})", projectionExpressionToString(std::move(*it)));
  }
  if(head.getName() == "Avg") {
    auto it = std::make_move_iterator(dynamics.begin());
    return fmt::format("avg({})", projectionExpressionToString(std::move(*it)));
  }
  throw std::runtime_error("Unknown operator: " + head.getName());
}
std::string projectionExpressionToString(Expression&& e) {
  return std::visit(boss::utilities::overload(
                        [](ComplexExpression&& expr) -> std::string {
                          return projectionExpressionToString(std::move(expr));
                        },
                        [](Symbol&& s) -> std::string {
                          auto name = std::move(s).getName();
                          std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                          return {std::move(name)};
                        },
                        [](std::string&& str) -> std::string { return str; },
                        [](auto&& v) -> std::string { return std::to_string(v); }),
                    std::move(e));
}

static std::vector<std::string> expressionToOneSideKeys(Expression&& e) {
  return std::visit(boss::utilities::overload(
                        [](ComplexExpression&& expr) -> std::vector<std::string> {
                          if(expr.getHead().getName() == "List" ||
                             expr.getHead().getName() == "By") {
                            auto [head, unused_, dynamics, unused2_] = std::move(expr).decompose();
                            std::vector<std::string> keys;
                            for(auto&& arg : dynamics) {
                              auto name = std::move(get<Symbol>(arg)).getName();
                              std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                              if(name == "desc") {
                                keys.back() += " desc";
                              } else {
                                keys.emplace_back(std::move(name));
                              }
                            }
                            return std::move(keys);
                          }
                          return {projectionExpressionToString(std::move(expr))};
                        },
                        [](Symbol&& s) -> std::vector<std::string> {
                          auto name = std::move(s).getName();
                          std::transform(name.begin(), name.end(), name.begin(), ::tolower);
                          return {std::move(name)};
                        },
                        [](auto&& v) -> std::vector<std::string> {
                          throw std::runtime_error("unexpected type for join/group key");
                        }),
                    std::move(e));
}

static std::tuple<std::vector<std::string>, std::vector<std::string>>
expressionToJoinKeys(ComplexExpression&& e) {
  auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
  auto it = std::make_move_iterator(dynamics.begin());
  auto buildKeys = expressionToOneSideKeys(std::move(*it++));
  auto probeKeys = expressionToOneSideKeys(std::move(*it++));
  return {std::move(buildKeys), std::move(probeKeys)};
}

static std::vector<std::string> expressionToProjections(ComplexExpression&& e) {
  if(e.getHead().getName() != "As") {
    return {projectionExpressionToString(std::move(e))};
  }
  auto [asHead, unused4_, asDynamics, unused5_] = std::move(e).decompose();
  std::vector<std::string> projections;
  for(auto asIt = std::make_move_iterator(asDynamics.begin());
      asIt != std::make_move_iterator(asDynamics.end()); ++asIt) {
    auto attrName = get<Symbol>(std::move(*asIt++)).getName();
    std::transform(attrName.begin(), attrName.end(), attrName.begin(), ::tolower);
    auto projStr = projectionExpressionToString(std::move(*asIt));
    projections.emplace_back(fmt::format("{} AS {}", projStr, attrName));
  }
  return projections;
}

static PlanBuilder buildOperatorPipeline(
    ComplexExpression&& e, std::vector<core::PlanNodeId>& scanIds, memory::MemoryPool& pool,
    std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator, int tableCnt) {
  if(e.getHead().getName() == "Table" || e.getHead().getName() == "Gather" ||
     e.getHead().getName() == "RadixPartition") {
    auto tableName = fmt::format("Table{}", tableCnt++);
    auto [rowDataVec, tableSchema, spanRowCountVec] = getColumns(std::move(std::move(e)), &pool);

    auto assignColumns = [](std::vector<std::string> names) {
      std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignmentsMap;
      assignmentsMap.reserve(names.size());
      for(auto& name : names) {
        assignmentsMap.emplace(name, std::make_shared<BossColumnHandle>(name));
      }
      return assignmentsMap;
    };
    auto assignmentsMap = assignColumns(tableSchema->names());

    core::PlanNodeId scanId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .startTableScan()
                    .outputType(tableSchema)
                    .tableHandle(std::make_shared<BossTableHandle>(
                        kBossConnectorId, tableName, tableSchema, rowDataVec, spanRowCountVec))
                    .assignments(assignmentsMap)
                    .endTableScan()
                    .capturePlanNodeId(scanId);
    scanIds.emplace_back(scanId);
    return std::move(plan);
  }
  if(e.getHead().getName() == "Project") {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt);
    auto asExpr = get<ComplexExpression>(std::move(*it++));
    return inputPlan.project(expressionToProjections(std::move(asExpr)));
  }
  if(e.getHead() == "Select"_) {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt);
    auto predStr = projectionExpressionToString(std::move(*it));
    return inputPlan.filter(predStr);
  }
  if(e.getHead() == "Join"_) {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto buildSideInputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)),
                                                    scanIds, pool, planNodeIdGenerator, tableCnt);
    auto probeSideInputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)),
                                                    scanIds, pool, planNodeIdGenerator, tableCnt);
    auto whereExpr = std::move(get<ComplexExpression>(*it));
    auto [unused3_, unused4_, whereDyns, unused5_] = std::move(whereExpr).decompose();
    auto [buildSideKeys, probeSideKeys] =
        expressionToJoinKeys(std::move(get<ComplexExpression>(whereDyns[0])));
    auto const& buildSideLayout = buildSideInputPlan.planNode()->outputType()->names();
    auto const& probeSideLayout = probeSideInputPlan.planNode()->outputType()->names();
    auto outputLayout = buildSideLayout;
    outputLayout.insert(outputLayout.end(), probeSideLayout.begin(), probeSideLayout.end());    
    return probeSideInputPlan.hashJoin(probeSideKeys, buildSideKeys, buildSideInputPlan.planNode(),
                                       "", outputLayout);
  }
  if(e.getHead() == "Group"_) {
    auto [unused0_, unused1_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto itEnd = std::make_move_iterator(dynamics.end());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt);
    auto secondArg = std::move(*it++);
    auto groupKeysStr =
        it == itEnd ? std::vector<std::string>{} : expressionToOneSideKeys(std::move(secondArg));
    auto asExpr = get<ComplexExpression>(it == itEnd ? std::move(secondArg) : std::move(*it));
    auto aggregates = expressionToProjections(std::move(asExpr));
    return inputPlan.singleAggregation(groupKeysStr, aggregates);
  }
  if(e.getHead() == "Order"_ || e.getHead() == "OrderBy"_ || e.getHead() == "Sort"_ ||
     e.getHead() == "SortBy"_) {
    auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt);
    auto groupKeysStr = expressionToOneSideKeys(std::move(*it));
    return inputPlan.orderBy(groupKeysStr, false);
  }
  if(e.getHead() == "Top"_ || e.getHead() == "TopN"_) {
    auto [head, unused_, dynamics, unused2_] = std::move(e).decompose();
    auto it = std::make_move_iterator(dynamics.begin());
    auto inputPlan = buildOperatorPipeline(get<ComplexExpression>(std::move(*it++)), scanIds, pool,
                                           planNodeIdGenerator, tableCnt);
    auto groupKeysStr = expressionToOneSideKeys(std::move(*it++));
    auto limit = std::holds_alternative<int32_t>(*it) ? std::get<int32_t>(std::move(*it))
                                                      : std::get<int64_t>(std::move(*it));
    return inputPlan.topN(groupKeysStr, limit, false);
  }
  throw std::runtime_error("Unknown relational operator: " + e.getHead().getName());
}

#ifdef DebugInfo
void veloxPrintResults(const std::vector<RowVectorPtr>& results) {
  std::cout << "Results:" << std::endl;
  bool printType = true;
  for(const auto& vector : results) {
    // Print RowType only once.
    if(printType) {
      std::cout << vector->type()->asRow().toString() << std::endl;
      printType = false;
    }
    for(vector_size_t i = 0; i < vector->size(); ++i) {
      std::cout << vector->toString(i) << std::endl;
    }
  }
}
#endif // DebugInfo

boss::Expression Engine::evaluate(boss::Expression&& e) {
  if(std::holds_alternative<ComplexExpression>(e)) {
    return evaluate(std::get<ComplexExpression>(std::move(e)));
  }
  return std::move(e);
}

boss::Expression Engine::evaluate(boss::ComplexExpression&& e) {
  if(e.getHead().getName() == "Set") {
    if(std::get<Symbol>(e.getDynamicArguments()[0]) == "maxThreads"_) {
      maxThreads = std::holds_alternative<int32_t>(e.getDynamicArguments()[1])
                       ? std::get<int32_t>(e.getDynamicArguments()[1])
                       : std::get<int64_t>(e.getDynamicArguments()[1]);
    }
    if(std::get<Symbol>(e.getDynamicArguments()[0]) == "NumSplits"_) {
      numSplits = std::holds_alternative<int32_t>(e.getDynamicArguments()[1])
                      ? std::get<int32_t>(e.getDynamicArguments()[1])
                      : std::get<int64_t>(e.getDynamicArguments()[1]);
    }
    if(std::get<Symbol>(e.getDynamicArguments()[0]) == "HashAdaptivityEnabled"_) {
      hashAdaptivityEnabled = std::get<bool>(e.getDynamicArguments()[1]);
    }
    return true;
  }

  static std::mutex m;
  auto pool = [this]() -> std::shared_ptr<memory::MemoryPool> {
    std::lock_guard const lock(m);
    auto& threadPool = threadPools_[std::this_thread::get_id()];
    if(!threadPool) {
      threadPool = memory::MemoryManager::getInstance()->addLeafPool();
    }
    return threadPool;
  }();

  auto params = std::make_unique<CursorParameters>();
  params->maxDrivers = maxThreads;
  params->copyResult = false;
  std::shared_ptr<folly::Executor> executor;
  if(maxThreads < 2) {
    params->singleThreaded = true;
  } else
  {
    executor = std::make_shared<folly::CPUThreadPoolExecutor>(std::thread::hardware_concurrency());
  }
  params->queryCtx = std::make_shared<core::QueryCtx>(
      executor.get(),
      core::QueryConfig{std::unordered_map<std::string, std::string>{
          {core::QueryConfig::kHashAdaptivityEnabled, hashAdaptivityEnabled ? "true" : "false"}}});
  std::unique_ptr<TaskCursor> cursor;

  boss::expressions::ExpressionArguments columns;
  auto evalAndAddOutputSpans = [&, this](auto&& e) {
    auto scanIds = std::vector<core::PlanNodeId>{};
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    int tableCnt = 0;
    auto plan = buildOperatorPipeline(std::move(e), scanIds, *pool, planNodeIdGenerator, tableCnt);
    params->planNode = plan.planNode();

    auto results = veloxRunQueryParallel(*params, cursor, scanIds, numSplits);
    if(!cursor) {
      throw std::runtime_error("Query terminated with error");
    }
#ifdef DebugInfo
    veloxPrintResults(results);
    std::cout << std::endl;
    auto& task = cursor->task();
    auto const& stats = task->taskStats();
    std::cout << printPlanWithStats(*params->planNode, stats, false) << std::endl;
#endif // DebugInfo
    if(!results.empty()) {
      for(auto& result : results) {
        // make sure that lazy vectors are computed
        result->loadedVector();
        // copy the result such that it is own by *OUR* memory pool
        // (this also ensure to flatten vectors wrapped in a dictionary)
        auto copy = BaseVector::create<RowVector>(result->type(), result->size(), pool.get());
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
        boss::expressions::ExpressionArguments args;
        args.emplace_back(ComplexExpression("List"_, {}, {}, std::move(spans)));
        columns.emplace_back(ComplexExpression(Symbol(name), {}, std::move(args), {}));
#else
        boss::expressions::ExpressionArguments args;
        args.emplace_back(Symbol(name));
        args.emplace_back(ComplexExpression("List"_, {}, {}, std::move(spans)));
        columns.emplace_back(ComplexExpression("Column"_, std::move(args)));
#endif // USE_NEW_TABLE_FORMAT
      }
    }
  };

  if(e.getHead().getName() == "Union") {
    auto [_, statics, dynamics, spans] = std::move(e).decompose();
    for(auto&& arg : dynamics) {
      evalAndAddOutputSpans(std::move(get<ComplexExpression>(arg)));
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
