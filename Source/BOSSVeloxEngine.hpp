#pragma once

#include <Expression.hpp>

#include <velox/common/memory/MemoryPool.h>

#include <memory>
#include <thread>
#include <unordered_map>

#ifdef _WIN32
extern "C" {
__declspec(dllexport) BOSSExpression* evaluate(BOSSExpression* e);
__declspec(dllexport) void reset();
}
#endif // _WIN32

#define SUPPORT_NEW_NUM_SPLITS
#define USE_NEW_TABLE_FORMAT

// #define DebugInfo

namespace boss::engines::velox {

class Engine {

public:
  Engine(Engine&) = delete;

  Engine& operator=(Engine&) = delete;

  Engine(Engine&&) = default;

  Engine& operator=(Engine&&) = delete;

  Engine();

  ~Engine();

  boss::Expression evaluate(boss::Expression&& e);
  boss::Expression evaluate(boss::ComplexExpression&& e);

private:
  std::unordered_map<std::thread::id, std::shared_ptr<facebook::velox::memory::MemoryPool>>
      threadPools_;

  int32_t maxThreads = 1;
  int32_t numSplits = 1;
  bool hashAdaptivityEnabled = true;
};

} // namespace boss::engines::velox
