#pragma once

#include <BOSS.hpp>
#include <Expression.hpp>

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
    Engine();

    Engine(Engine &) = delete;

    Engine &operator=(Engine &) = delete;

    Engine(Engine &&) = default;

    Engine &operator=(Engine &&) = delete;

//  Engine() = default;
    ~Engine() = default;

  boss::Expression evaluate(boss::Expression&& e);

private:
  bool memoryMapped = true;
  std::unordered_map<std::string, boss::ComplexExpression> tables;

};

} // namespace boss::engines::velox
