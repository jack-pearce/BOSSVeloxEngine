#include "VectorBridge.h"

#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace boss::engines::velox {
    void mockArrayRelease(BossArray *) {}

    BossArray makeBossArray(const void *buffers, int64_t length) {
      return BossArray{
              .length = length,
              .buffers = buffers,
              .release = mockArrayRelease,
      };
    }

// Optionally, holds shared_ptrs pointing to the BossArray object that
// holds the buffer object that describes the BossArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
    struct BufferViewReleaser {
        BufferViewReleaser() : BufferViewReleaser(nullptr) {}

        explicit BufferViewReleaser(
                std::shared_ptr<BossArray> bossArray)
                : arrayReleaser_(std::move(bossArray)) {}

        void addRef() const {}

        void release() const {}

    private:
        const std::shared_ptr<BossArray> arrayReleaser_;
    };

// Wraps a naked pointer using a Velox buffer view, without copying it. Adding a
// dummy releaser as the buffer lifetime is fully controled by the client of the API.
    BufferPtr wrapInBufferViewAsViewer(const void *buffer, size_t length) {
      static const BufferViewReleaser kViewerReleaser;
      return BufferView<BufferViewReleaser>::create(
              static_cast<const uint8_t *>(buffer), length, kViewerReleaser);
    }

    using WrapInBufferViewFunc =
            std::function<BufferPtr(const void *buffer, size_t length)>;

    // Dispatch based on the type.
    template<TypeKind kind>
    VectorPtr createFlatVector(
            memory::MemoryPool *pool,
            const TypePtr &type,
            BufferPtr nulls,
            size_t length,
            BufferPtr values) {
      using T = typename TypeTraits<kind>::NativeType;
      return std::make_shared<FlatVector<T>>
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
          return BOOLEAN();
        case 1:
          return BIGINT();
        case 2:
          return DOUBLE();
        case 3:
          return VARCHAR();
        default:
          break;
      }
      VELOX_USER_FAIL(
              "Unable to convert '{}' BossType format type to Velox.", bossType);
    }

    VectorPtr importFromBossImpl(
            BossType bossType,
            const BossArray &bossArray,
            memory::MemoryPool *pool,
            WrapInBufferViewFunc wrapInBufferView) {
      VELOX_USER_CHECK_NOT_NULL(bossArray.release, "bossArray was released.");
      VELOX_CHECK_GE(bossArray.length, 0, "Array length needs to be non-negative.");

      // First parse and generate a Velox type.
      auto type = importFromBossType(bossType);

      // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
      // needs to be at least one bit per element.
      BufferPtr nulls = nullptr;

      // Other primitive types.
      VELOX_CHECK(
              type->isPrimitiveType(),
              "Conversion of '{}' from Boss not supported yet.",
              type->toString());

      // Wrap the values buffer into a Velox BufferView - zero-copy.
      auto values = wrapInBufferView(
              bossArray.buffers, bossArray.length * type->cppSizeInBytes());

      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
              createFlatVector,
              type->kind(),
              pool,
              type,
              nulls,
              bossArray.length,
              values);
    }

    VectorPtr importFromBossAsViewer(
            BossType bossType,
            const BossArray &bossArray,
            memory::MemoryPool *pool) {
      return importFromBossImpl(
              bossType, bossArray, pool, wrapInBufferViewAsViewer);
    }

} // namespace boss::engines::velox