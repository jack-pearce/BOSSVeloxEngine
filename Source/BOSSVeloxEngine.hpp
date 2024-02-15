#pragma once

#include "BOSSQueryBuilder.h"
#include <unordered_map>

#ifdef _WIN32
extern "C" {
__declspec(dllexport) BOSSExpression* evaluate(BOSSExpression* e);
__declspec(dllexport) void reset();
}
#endif // _WIN32

namespace boss::engines::velox {

    class Engine {

    public:

        Engine(Engine &) = delete;

        Engine &operator=(Engine &) = delete;

        Engine(Engine &&) = default;

        Engine &operator=(Engine &&) = delete;

        Engine();

        ~Engine();

        boss::Expression evaluate(boss::Expression &&e);
        boss::Expression evaluate(boss::ComplexExpression &&e);

    private:
        std::shared_ptr<folly::Executor> executor_; // execute Velox physical plan, e.g. projection
        std::shared_ptr<memory::MemoryPool> pool_;
        std::unique_ptr<CursorParameters> params_;
        std::unique_ptr<TaskCursor> cursor_;
    };

} // namespace boss::engines::velox
