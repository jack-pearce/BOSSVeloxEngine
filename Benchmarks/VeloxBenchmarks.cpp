#include "ITTNotifySupport.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <benchmark/benchmark.h>

#include <cmath>
#include <future>
#include <iostream>
#include <random>

using boss::utilities::operator""_; // NOLINT(misc-unused-using-decls) clang-tidy bug

namespace utilities {
static boss::Expression injectDebugInfoToSpans(boss::Expression&& expr) {
  return std::visit(
      boss::utilities::overload(
          [&](boss::ComplexExpression&& e) -> boss::Expression {
            auto [head, unused_, dynamics, spans] = std::move(e).decompose();
            boss::ExpressionArguments debugDynamics;
            debugDynamics.reserve(dynamics.size() + spans.size());
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           std::back_inserter(debugDynamics), [](auto&& arg) {
                             return injectDebugInfoToSpans(std::forward<decltype(arg)>(arg));
                           });
            std::transform(
                std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                std::back_inserter(debugDynamics), [](auto&& span) {
                  return std::visit(
                      [](auto&& typedSpan) -> boss::Expression {
                        using Element = typename std::decay_t<decltype(typedSpan)>::element_type;
                        return boss::ComplexExpressionWithStaticArguments<std::string, int64_t>(
                            "Span"_, {typeid(Element).name(), typedSpan.size()}, {}, {});
                      },
                      std::forward<decltype(span)>(span));
                });
            return boss::ComplexExpression(std::move(head), {}, std::move(debugDynamics), {});
          },
          [](auto&& otherTypes) -> boss::Expression { return otherTypes; }),
      std::move(expr));
}
}; // namespace utilities

static auto const vtune = VTuneAPIInterface{"VeloxBOSS"};

static auto& librariesToTest() {
  static std::vector<std::string> libraries;
  return libraries;
};

void runRadixJoin(benchmark::State& state, size_t buildsize, size_t probesize, size_t numPartitions,
                  bool hashAdaptivity, bool useDictionary, bool useUnion, bool multithreaded) {
  auto eval = [](auto&& expression) {
    boss::expressions::ExpressionSpanArguments spans;
    spans.emplace_back(boss::expressions::Span<std::string>(librariesToTest()));
    return boss::evaluate("EvaluateInEngines"_(
        boss::ComplexExpression("List"_, {}, {}, std::move(spans)), std::move(expression)));
  };

  auto error_handling = [](auto&& result) {
    if(!holds_alternative<boss::ComplexExpression>(result)) {
      return false;
    }
    if(get<boss::ComplexExpression>(result).getHead() == "Table"_) {
      return false;
    }
    if(get<boss::ComplexExpression>(result).getHead() == "List"_) {
      return false;
    }
    std::cout << "Error: " << utilities::injectDebugInfoToSpans(std::move(result)) << std::endl;
    return true;
  };

  eval("Set"_("HashAdaptivityEnabled"_, hashAdaptivity));

  eval("Set"_("MaxDrivers"_, (int32_t)1));

  eval("Set"_("NumSplits"_, (int32_t)(1000 / numPartitions)));

  eval("Set"_("BatchSize"_, (int32_t)1024)); // ideally should set this instead of numSplits
                                             // not implemented yet

  auto custDatasize = probesize;
  auto orderDatasize = buildsize;

  size_t custPartitionSize = custDatasize / numPartitions;
  size_t orderPartitionSize = orderDatasize / numPartitions;

  auto custkeyVec = std::vector<int64_t>(custDatasize);
  std::iota(custkeyVec.begin(), custkeyVec.end(), 1);

  auto orderKeyVec = std::vector<int64_t>(orderDatasize);
  auto orderDateVec = std::vector<int32_t>(orderDatasize);
  auto oCustKeyVec = std::vector<int64_t>(orderDatasize);
  std::iota(oCustKeyVec.begin(), oCustKeyVec.end(), 1);
  auto oShipPriorityVec = std::vector<int32_t>(orderDatasize);

  std::random_device dev;
  std::mt19937 rengine(dev());

  bool failed = false;
  for(auto _ : state) { // NOLINT
    if(failed) {
      continue;
    }
    state.PauseTiming();
    // INIT
    boss::expressions::ExpressionArguments joins;
    joins.reserve(numPartitions);
    for(int custOffset = 0, orderOffset = 0, i = 0; i < numPartitions; ++i) {
      std::shuffle(custkeyVec.begin() + custOffset,
                   custkeyVec.begin() + custOffset + custPartitionSize, rengine);
      std::shuffle(oCustKeyVec.begin() + orderOffset,
                   oCustKeyVec.begin() + orderOffset + orderPartitionSize, rengine);

      boss::expressions::ExpressionSpanArguments custKeySpans;
      if(useDictionary) {
        custKeySpans.emplace_back(boss::Span<int64_t>(custkeyVec));
      } else {
        custKeySpans.emplace_back(
            boss::Span<int64_t>(custkeyVec).subspan(custOffset, custPartitionSize));
      }

      auto filteredCustomer = "Table"_("Column"_(
          "C_CUSTKEY"_, boss::ComplexExpression("List"_, {}, {}, std::move(custKeySpans))));

      boss::expressions::ExpressionSpanArguments orderKeySpans;
      if(useDictionary) {
        orderKeySpans.emplace_back(boss::Span<int64_t>(orderKeyVec));
      } else {
        orderKeySpans.emplace_back(
            boss::Span<int64_t>(orderKeyVec).subspan(orderOffset, orderPartitionSize));
      }

      boss::expressions::ExpressionSpanArguments orderDateSpans;
      if(useDictionary) {
        orderDateSpans.emplace_back(boss::Span<int32_t>(orderDateVec));
      } else {
        orderDateSpans.emplace_back(
            boss::Span<int32_t>(orderDateVec).subspan(orderOffset, orderPartitionSize));
      }

      boss::expressions::ExpressionSpanArguments oCustkeySpans;
      if(useDictionary) {
        oCustkeySpans.emplace_back(boss::Span<int64_t>(oCustKeyVec));
      } else {
        oCustkeySpans.emplace_back(
            boss::Span<int64_t>(oCustKeyVec).subspan(orderOffset, orderPartitionSize));
      }

      boss::expressions::ExpressionSpanArguments oShipPrioritySpans;
      if(useDictionary) {
        oShipPrioritySpans.emplace_back(boss::Span<int32_t>(oShipPriorityVec));
      } else {
        oShipPrioritySpans.emplace_back(
            boss::Span<int32_t>(oShipPriorityVec).subspan(orderOffset, orderPartitionSize));
      }

      auto filteredOrders = "Table"_(
          "Column"_("O_ORDERKEY"_,
                    boss::ComplexExpression("List"_, {}, {}, std::move(orderKeySpans))),
          "Column"_("O_ORDERDATE"_,
                    boss::ComplexExpression("List"_, {}, {}, std::move(orderDateSpans))),
          "Column"_("O_CUSTKEY"_,
                    boss::ComplexExpression("List"_, {}, {}, std::move(oCustkeySpans))),
          "Column"_("O_SHIPPRIORITY"_,
                    boss::ComplexExpression("List"_, {}, {}, std::move(oShipPrioritySpans))));

      if(useDictionary) {
        auto custPositions = std::vector<int32_t>(custPartitionSize);
        std::iota(custPositions.begin(), custPositions.end(), custOffset);
        auto orderPositions = std::vector<int32_t>(orderPartitionSize);
        std::iota(orderPositions.begin(), orderPositions.end(), orderOffset);

        joins.emplace_back("Project"_(
            "Join"_("RadixPartitions"_(std::move(filteredCustomer),
                                       "List"_(boss::Span<int32_t>{std::move(custPositions)})),
                    "RadixPartitions"_(std::move(filteredOrders),
                                       "List"_(boss::Span<int32_t>{std::move(orderPositions)})),
                    "Where"_("Equal"_("C_CUSTKEY"_, "O_CUSTKEY"_))),
            "As"_("O_ORDERKEY"_, "O_ORDERKEY"_, "O_ORDERDATE"_, "O_ORDERDATE"_, "O_CUSTKEY"_,
                  "O_CUSTKEY"_, "O_SHIPPRIORITY"_, "O_SHIPPRIORITY"_)));
      } else {
        joins.emplace_back(
            "Project"_("Join"_(std::move(filteredCustomer), std::move(filteredOrders),
                               "Where"_("Equal"_("C_CUSTKEY"_, "O_CUSTKEY"_))),
                       "As"_("O_ORDERKEY"_, "O_ORDERKEY"_, "O_ORDERDATE"_, "O_ORDERDATE"_,
                             "O_CUSTKEY"_, "O_CUSTKEY"_, "O_SHIPPRIORITY"_, "O_SHIPPRIORITY"_)));
      }

      custOffset += custPartitionSize;
      orderOffset += orderPartitionSize;
    }
    if(useUnion) {
      auto unionQuery = boss::ComplexExpression("Union"_, std::move(joins));
      joins.clear();
      joins.emplace_back(std::move(unionQuery));
    }
    vtune.startSampling("RadixJoin");
    state.ResumeTiming();
    // RUN
    if(multithreaded) {
      auto maxThreads = std::thread::hardware_concurrency();
      std::vector<std::future<boss::Expression>> futures;
      futures.reserve(maxThreads);
      auto getResults = [&]() {
        for(auto&& future : futures) {
          auto dummyResult = std::move(future.get());
          benchmark::DoNotOptimize(dummyResult);
          if(error_handling(dummyResult)) {
            failed = true;
          }
        }
        futures.clear();
      };
      for(int i = 0; i < joins.size(); ++i) {
        if(failed) {
          break;
        }
        if(futures.size() >= maxThreads) {
          getResults();
        }
        futures.emplace_back(std::async(
            std::launch::async,
            [&joins, &eval](int idx) {
              return eval(std::move(joins[idx]));
            },
            i));
      }
      getResults();
    } else {
      for(auto&& join : joins) {
        auto dummyResult = eval(std::move(join));
        benchmark::DoNotOptimize(dummyResult);
        if(error_handling(dummyResult)) {
          failed = true;
          break;
        }
      }
    }
    vtune.stopSampling();
  }
}

template <typename... Args>
benchmark::internal::Benchmark* RegisterBenchmarkNolint([[maybe_unused]] Args... args) {
#ifdef __clang_analyzer__
  // There is not way to disable clang-analyzer-cplusplus.NewDeleteLeaks
  // even though it is perfectly safe. Let's just please clang analyzer.
  return nullptr;
#else
  return benchmark::RegisterBenchmark(args...);
#endif
}
template <typename Func>
void RegisterTest(std::string const& name, Func&& func, float scaleFactor,
                  unsigned int numPartitions, bool groupedExecution) {
  std::ostringstream testName;
  testName << name;
  testName << "/SF:" << scaleFactor;
  testName << "/Parts:" << numPartitions;
  testName << "/Grouped:" << groupedExecution;
  RegisterBenchmarkNolint(testName.str().c_str(), func, scaleFactor, numPartitions,
                          groupedExecution);
}

template <typename Func>
void RegisterBOSSTest(std::string const& name, Func&& func, size_t buildsize, size_t probesize,
                      size_t numPartitions, bool hashAdaptivity, bool useDictionary, bool useUnion,
                      bool multithreaded) {
  std::ostringstream testName;
  testName << name;
  testName << "/buildsize:" << buildsize;
  testName << "/probesize:" << probesize;
  testName << "/Parts:" << numPartitions;
  testName << "/analyze:" << (int)hashAdaptivity;
  testName << "/dict:" << (int)useDictionary;
  testName << "/union:" << (int)useUnion;
  testName << "/mt:" << (int)multithreaded;
  RegisterBenchmarkNolint(testName.str().c_str(), func, buildsize, probesize, numPartitions,
                          hashAdaptivity, useDictionary, useUnion, multithreaded);
}

void initAndRunBenchmarks(int argc, char** argv) {
  for(auto buildsize : std::vector<size_t>{100'000, 1'000'000, 10'000'000, 100'000'000}) {
    for(auto probesize : std::vector<size_t>{100'000, 1'000'000, 10'000'000, 100'000'000}) {
      if(buildsize > probesize) {
        continue;
      }
      for(auto numPartitions : std::vector<size_t>{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}) {
        for(bool hashAdaptivity : std::vector<bool>{false, true}) {
          for(bool useDictionary : std::vector<bool>{false, true}) {
            for(bool useUnion : std::vector<bool>{false, true}) {
              for(bool multithreaded : std::vector<bool>{false, true}) {
                if(multithreaded && useUnion) {
                  continue;
                }
                RegisterBOSSTest("RadixJoin", runRadixJoin, buildsize, probesize, numPartitions,
                                 hashAdaptivity, useDictionary, useUnion, multithreaded);
              }
            }
          }
        }
      }
    }
  }
  // initialise and run google benchmark
  ::benchmark::Initialize(&argc, argv, ::benchmark::PrintDefaultHelp);
  ::benchmark::RunSpecifiedBenchmarks();
}

int main(int argc, char** argv) {
  for(int i = 0; i < argc; ++i) {
    if(std::string("--library") == argv[i]) {
      if(++i < argc) {
        librariesToTest().emplace_back(argv[i]);
      }
    }
  }

  try {
    initAndRunBenchmarks(argc, argv);
  } catch(std::exception& e) {
    std::cerr << "caught exception in main: " << e.what() << std::endl;
    return EXIT_FAILURE;
  } catch(...) {
    std::cerr << "unhandled exception." << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
