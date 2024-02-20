#pragma once

#include <ExpressionUtilities.hpp>

#include "BOSSCoreTmp.h"
#include "BridgeVelox.h"

using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Expression;
using boss::Span;
using boss::Symbol;
using boss::expressions::generic::isComplexExpression;

namespace boss {
using SpanInputs =
    std::variant<std::vector<std::int32_t>, std::vector<std::int64_t>, std::vector<std::double_t>,
                 std::vector<std::string>, std::vector<Symbol>>;
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

  if constexpr(std::is_same_v<T, int32_t>) {
    return createVeloxVector(BossType::bINTEGER);
  } else if constexpr(std::is_same_v<T, int64_t>) {
    return createVeloxVector(BossType::bBIGINT);
  } else if constexpr(std::is_same_v<T, double_t>) {
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

static void yearToInt32(std::vector<std::string>& projectionList) {
  auto out = fmt::format("cast(((cast({} AS DOUBLE) + 719563.285) / 365.265) AS INTEGER)",
                         projectionList.back());
  projectionList.pop_back();
  projectionList.pop_back();
  projectionList.push_back(out);
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

} // namespace boss::engines::velox