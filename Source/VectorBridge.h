#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

using namespace facebook::velox;
using namespace facebook::velox::memory;

namespace boss::engines::velox {
/**
 *     bool = 0, long = 1, double = 2 , ::std::string = 3, Symbol = 4
 */
    enum BossType {
        bBOOL = 0,
        bBIGINT,
        bDOUBLE,
        bSTRING
    };

    struct BossArray {
        // Array data description
        int64_t length;
        const void *buffers;

        // Release callback
        void (*release)(struct BossArray *);
    };

    VectorPtr importFromBossAsViewer(
            BossType bossType,
            const BossArray &bossArray,
            memory::MemoryPool *pool);

    BossArray makeBossArray(
            const void *buffers,
            int64_t length);

} // namespace boss::engines::velox
