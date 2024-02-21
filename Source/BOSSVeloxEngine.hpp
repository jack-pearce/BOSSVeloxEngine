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
        std::vector<std::shared_ptr<memory::MemoryPool>> pools_;

        int32_t maxThreads = 1;
        int32_t numSplits = 64;
        bool hashAdaptivityEnabled = true;
    };

} // namespace boss::engines::velox
