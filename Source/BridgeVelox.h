#pragma once

#include <ExpressionUtilities.hpp>
#include <BOSS.hpp>
#include <Expression.hpp>
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

//#define DebugInfo
using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace boss::engines::velox {
    using VeloxExpressionSystem = ExtensibleExpressionSystem<>;
    using ExpressionSpanArgument = VeloxExpressionSystem::ExpressionSpanArgument;
    /**
 *     bool = 0, long = 1, double = 2, int = 3
 */
    enum BossType {
        bBOOL = 0,
        bBIGINT,
        bDOUBLE,
        bINTEGER
    };

    struct BossArray {
        explicit BossArray(int64_t span_size, void *span_begin, ExpressionSpanArgument &&span)
                : length(span_size),
                  buffers(span_begin),
                  holdSpan(std::move(span)) {
        }

        BossArray(BossArray const &bossArray) noexcept = delete;

        BossArray(BossArray &&bossArray) noexcept {
            length = bossArray.length;
            buffers = bossArray.buffers;
            holdSpan = std::move(bossArray.holdSpan);
        }

        ExpressionSpanArgument holdSpan{};
        int64_t length;
        const void *buffers;
    };

    VectorPtr importFromBossAsOwner(BossType bossType, BossArray &bossArray, memory::MemoryPool *pool);

    BufferPtr importFromBossAsOwnerBuffer(BossArray &bossArray);

    std::vector<RowVectorPtr> veloxRunQuery(const CursorParameters &params, std::unique_ptr<TaskCursor> &cursor);

    void veloxPrintResults(const std::vector<RowVectorPtr> &results);

    RowVectorPtr makeRowVectorNoCopy(std::vector<std::string> childNames,
                                     std::vector<VectorPtr> children, memory::MemoryPool *pool);

} // namespace boss::engines::velox