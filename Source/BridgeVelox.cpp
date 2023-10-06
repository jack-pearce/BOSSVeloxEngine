//Data structures and functions in this file are referenced/copied from Velox prototype for Arrow Velox conversion.

#include "BridgeVelox.h"

#include <utility>

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace boss::engines::velox {

// Optionally, holds shared_ptrs pointing to the BossArray object that
// holds the buffer object that describes the BossArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
    struct BufferViewReleaser {

        explicit BufferViewReleaser(
                std::shared_ptr<BossArray> bossArray)
                : arrayReleaser_(std::move(bossArray)) {}

        void addRef() const {}

        void release() const {}

    private:
        const std::shared_ptr<BossArray> arrayReleaser_;
    };

    // Wraps a naked pointer using a Velox buffer view, without copying it. This
    // buffer view uses shared_ptr to manage reference counting and releasing for
    // the BossArray object
    BufferPtr wrapInBufferViewAsOwner(
            const void *buffer,
            size_t length,
            std::shared_ptr<BossArray> arrayReleaser) {
        return BufferView<BufferViewReleaser>::create(
                static_cast<const uint8_t *>(buffer), length, {BufferViewReleaser(std::move(arrayReleaser))});
    }

    // Dispatch based on the type.
    template<TypeKind kind>
    VectorPtr createFlatVector(
            memory::MemoryPool *pool,
            const TypePtr &type,
            BufferPtr nulls,
            size_t length,
            BufferPtr values) {
        using T = typename TypeTraits<kind>::NativeType;
        return std::make_shared<FlatVector<T >>
                (
                        pool,
                        type,
                        nulls,
                        length,
                        values,
                        std::vector<BufferPtr>(),
                        SimpleVectorStats<T>{},
                        std::nullopt,
                        std::nullopt);
    }

    TypePtr importFromBossType(BossType &bossType) {
        switch (bossType) {
            case 0:
                return INTEGER();
            case 1:
                return BIGINT();
            case 2:
                return DOUBLE();
            default:
                break;
        }
        VELOX_USER_FAIL(
                "Unable to convert '{}' BossType format type to Velox.", bossType)
    }

    VectorPtr importFromBossAsOwner(
            BossType bossType,
            BossArray &bossArray,
            memory::MemoryPool *pool) {
        VELOX_CHECK_GE(bossArray.length, 0, "Array length needs to be non-negative.")

        // First parse and generate a Velox type.
        auto type = importFromBossType(bossType);

        // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
        // needs to be at least one bit per element.
        BufferPtr nulls = nullptr;

        // Other primitive types.
        VELOX_CHECK(
                type->isPrimitiveType(),
                "Conversion of '{}' from Boss not supported yet.",
                type->toString())

        // Wrap the values buffer into a Velox BufferView - zero-copy.
        const auto *buffer = bossArray.buffers;
        auto length = bossArray.length * type->cppSizeInBytes();
        std::shared_ptr<BossArray> const arrayReleaser(new BossArray(std::move(bossArray)));
        auto values = wrapInBufferViewAsOwner(buffer, length, arrayReleaser);

        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
                createFlatVector,
                type->kind(),
                pool,
                type,
                nulls,
                bossArray.length,
                values);
    }

    BufferPtr importFromBossAsOwnerBuffer(BossArray &bossArray) {
        VELOX_CHECK_GE(bossArray.length, 0, "Array length needs to be non-negative.")

        // Wrap the values buffer into a Velox BufferView - zero-copy.
        const auto *buffer = bossArray.buffers;
        auto length = bossArray.length * 4;
        std::shared_ptr<BossArray> const arrayReleaser(new BossArray(std::move(bossArray)));
        return wrapInBufferViewAsOwner(buffer, length, arrayReleaser);;
    }

    std::vector<RowVectorPtr> myReadCursor(const CursorParameters& params,
                                           std::unique_ptr<TaskCursor>& cursor,
                                           std::function<void(exec::Task*)> addSplits) {
        cursor = std::make_unique<TaskCursor>(params);
        // 'result' borrows memory from cursor so the life cycle must be shorter.
        std::vector<RowVectorPtr> result;
        auto* task = cursor->task().get();
        addSplits(task);

        while(cursor->moveNext()) {
            result.push_back(cursor->current());
            addSplits(task);
        }

        return std::move(result);
    }

    std::vector<RowVectorPtr> veloxRunQueryParallel(const CursorParameters& params,
                                                    std::unique_ptr<TaskCursor>& cursor,
                                                    std::vector<core::PlanNodeId> scanIds,
                                                    const int numSplits) {
        try {
            bool noMoreSplits = false;
            auto addSplits = [&](exec::Task* task) {
              if(!noMoreSplits) {
                for(auto& scanId : scanIds) {
                  std::vector<exec::Split> splits;
                  splits.reserve(numSplits);
                  for(size_t i = 0; i < numSplits; ++i) {
                    splits.emplace_back(exec::Split(
                        std::make_shared<BossConnectorSplit>(kBossConnectorId, numSplits, i)));
                  }
                  for(auto& split : splits) {
                    task->addSplit(scanId, exec::Split(split));
                  }
                  task->noMoreSplits(scanId);
                }
              }
              noMoreSplits = true;
            };
            auto result = myReadCursor(params, cursor, addSplits);
            return result;
        } catch(const std::exception& e) {
            LOG(ERROR) << "Query terminated with: " << e.what();
            return {};
        }
    }

    // for debug usage
    void veloxPrintResults(const std::vector<RowVectorPtr> &results) {
        std::cout << "Results:" << std::endl;
        bool printType = true;
        for (const auto &vector: results) {
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

    RowVectorPtr makeRowVectorNoCopy(std::vector<std::string> childNames,
                                     std::vector<VectorPtr> children, memory::MemoryPool *pool) {
        std::vector<std::shared_ptr<const Type>> childTypes;
        childTypes.resize(children.size());
        for (int i = 0; i < children.size(); i++) {
            childTypes[i] = children[i]->type();
        }
        auto rowType = ROW(std::move(childNames), std::move(childTypes));
        const size_t vectorSize = children.empty() ? 0 : children.front()->size();

        return std::make_shared<RowVector>(pool, rowType, BufferPtr(nullptr), vectorSize,
                                           children);
    }

} // namespace boss::engines::velox