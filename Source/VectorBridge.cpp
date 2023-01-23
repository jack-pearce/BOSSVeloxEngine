#include "VectorBridge.h"

#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {

namespace {

// Structure that will hold the buffers needed by VeloxArray. This is opaquely
// carried by VeloxArray.private_data
class VeloxToBossBridgeHolder {
 public:

  void setBuffer(const BufferPtr& buffer) {
    bufferPtrs_ = buffer;
    if (buffer) {
      buffers_ = buffer->as<void>();
    }
  }

  const void* getBossBuffers() {
    return buffers_;
  }

  // Allocates space for `numChildren` VeloxArray pointers.
  void resizeChildren(size_t numChildren) {
    childrenPtrs_.resize(numChildren);
    children_ = (numChildren > 0)
        ? std::make_unique<VeloxArray*[]>(sizeof(VeloxArray*) * numChildren)
        : nullptr;
  }

  // Allocates and properly acquires buffers for a child VeloxArray structure.
  VeloxArray* allocateChild(size_t i) {
    VELOX_CHECK_LT(i, childrenPtrs_.size());
    childrenPtrs_[i] = std::make_unique<VeloxArray>();
    children_[i] = childrenPtrs_[i].get();
    return children_[i];
  }

  // Returns the pointer to be used in the parent VeloxArray structure.
  VeloxArray** getChildrenArrays() {
    return children_.get();
  }

 private:
  // Holds the pointers to the boss buffers.
  const void* buffers_;

  // Holds ownership over the Buffers being referenced by the buffers vector
  // above.
  BufferPtr bufferPtrs_;

  // Auxiliary buffers to hold ownership over VeloxArray children structures.
  std::vector<std::unique_ptr<VeloxArray>> childrenPtrs_;

  // Array that will hold pointers to the structures above - to be used by
  // VeloxArray.children
  std::unique_ptr<VeloxArray*[]> children_;
};

// Release function for VeloxArray. Boss standard requires it to recurse down
// to children and dictionary arrays, and set release and private_data to null
// to signal it has been released.
static void bridgeRelease(VeloxArray* veloxArray) {
  if (!veloxArray || !veloxArray->release) {
    return;
  }

  // Recurse down to release children arrays.
  for (int64_t i = 0; i < veloxArray->n_children; ++i) {
    VeloxArray* child = veloxArray->children[i];
    if (child != nullptr && child->release != nullptr) {
      child->release(child);
      VELOX_CHECK_NULL(child->release);
    }
  }

  // Destroy the current holder.
  auto* bridgeHolder =
      static_cast<VeloxToBossBridgeHolder*>(veloxArray->private_data);
  delete bridgeHolder;

  // Finally, mark the array as released.
  veloxArray->release = nullptr;
  veloxArray->private_data = nullptr;
}

// Returns the Boss C data interface format type for a given Velox type.
const char* exportBossFormatStr(
    const TypePtr& type,
    std::string& formatBuffer) {
  switch (type->kind()) {
    // Scalar types.
    case TypeKind::BOOLEAN:
      return "b"; // boolean
    case TypeKind::TINYINT:
      return "c"; // int8
    case TypeKind::SMALLINT:
      return "s"; // int16
    case TypeKind::INTEGER:
      return "i"; // int32
    case TypeKind::BIGINT:
      return "l"; // int64
    case TypeKind::REAL:
      return "f"; // float32
    case TypeKind::DOUBLE:
      return "g"; // float64
    // Decimal types encode the precision, scale values.
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL: {
      const auto& [precision, scale] = getDecimalPrecisionScale(*type);
      formatBuffer = fmt::format("d:{},{}", precision, scale);
      return formatBuffer.c_str();
    }
    // We always map VARCHAR and VARBINARY to the "small" version (lower case
    // format string), which uses 32 bit offsets.
    case TypeKind::VARCHAR:
      return "u"; // utf-8 string
    case TypeKind::VARBINARY:
      return "z"; // binary

    case TypeKind::TIMESTAMP:
      // TODO: need to figure out how we'll map this since in Velox we currently
      // store timestamps as two int64s (epoch in sec and nanos).
      return "ttn"; // time64 [nanoseconds]
    case TypeKind::DATE:
      return "tdD"; // date32[days]
    // Complex/nested types.
    case TypeKind::ARRAY:
      static_assert(sizeof(vector_size_t) == 4);
      return "+l"; // list
    case TypeKind::MAP:
      return "+m"; // map
    case TypeKind::ROW:
      return "+s"; // struct

    default:
      VELOX_NYI("Unable to map type '{}' to BossType.", type->kind());
  }
}

// A filter representation that can also keep the order.
struct Selection {
  explicit Selection(vector_size_t total) : total_(total) {}

  // Whether filtering or reorder should be applied to the original elements.
  bool changed() const {
    return static_cast<bool>(ranges_);
  }

  template <typename F>
  void apply(F&& f) const {
    if (changed()) {
      for (auto [offset, size] : *ranges_) {
        for (vector_size_t i = 0; i < size; ++i) {
          f(offset + i);
        }
      }
    } else {
      for (vector_size_t i = 0; i < total_; ++i) {
        f(i);
      }
    }
  }

  vector_size_t count() const {
    if (!changed()) {
      return total_;
    }
    vector_size_t ans = 0;
    for (auto [_, size] : *ranges_) {
      ans += size;
    }
    return ans;
  }

  void clearAll() {
    ranges_ = std::vector<std::pair<vector_size_t, vector_size_t>>();
  }

  void addRange(vector_size_t offset, vector_size_t size) {
    VELOX_DCHECK(ranges_);
    ranges_->emplace_back(offset, size);
  }

 private:
  std::optional<std::vector<std::pair<vector_size_t, vector_size_t>>> ranges_;
  vector_size_t total_;
};

void gatherFromBuffer(
    const Type& type,
    const Buffer& buf,
    const Selection& rows,
    Buffer& out) {
  auto src = buf.as<uint8_t>();
  auto dst = out.asMutable<uint8_t>();
  vector_size_t j = 0; // index into dst
  if (type.kind() == TypeKind::BOOLEAN) {
    rows.apply([&](vector_size_t i) {
      bits::setBit(dst, j++, bits::isBitSet(src, i));
    });
  } else if (type.kind() == TypeKind::SHORT_DECIMAL) {
    rows.apply([&](vector_size_t i) {
      auto decimalSrc = buf.as<UnscaledShortDecimal>();
      int128_t value = decimalSrc[i].unscaledValue();
      memcpy(dst + (j++) * sizeof(int128_t), &value, sizeof(int128_t));
    });
  } else {
    auto typeSize = type.cppSizeInBytes();
    rows.apply([&](vector_size_t i) {
      memcpy(dst + (j++) * typeSize, src + i * typeSize, typeSize);
    });
  }
}

void exportValues(
    const BaseVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder) {
  // Short decimals need to be converted to 128 bit values as they are mapped
  // to Boss Decimal128.
  if (!rows.changed() && !vec.type()->isShortDecimal()) {
    holder.setBuffer(vec.values());
    return;
  }
  auto size = vec.type()->isShortDecimal() ? sizeof(int128_t)
                                           : vec.type()->cppSizeInBytes();
  auto values = vec.type()->isBoolean()
      ? AlignedBuffer::allocate<bool>(out.length, pool)
      : AlignedBuffer::allocate<uint8_t>(
            checkedMultiply<size_t>(out.length, size), pool);
  gatherFromBuffer(*vec.type(), *vec.values(), rows, *values);
  holder.setBuffer(values);
}

void exportFlat(
    const BaseVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder) {
  out.n_children = 0;
  out.children = nullptr;
  switch (vec.typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL:
      exportValues(vec, rows, out, pool, holder);
      break;
    default:
      VELOX_NYI(
          "Conversion of FlatVector of {} is not supported yet.",
          vec.typeKind());
  }
}

void exportBase(
    const BaseVector&,
    const Selection&,
    VeloxArray&,
    memory::MemoryPool*);

void exportRows(
    const RowVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder) {
  holder.resizeChildren(vec.childrenSize());
  out.n_children = vec.childrenSize();
  out.children = holder.getChildrenArrays();
  for (column_index_t i = 0; i < vec.childrenSize(); ++i) {
    try {
      exportBase(
          *vec.childAt(i)->loadedVector(),
          rows,
          *holder.allocateChild(i),
          pool);
    } catch (const VeloxException&) {
      for (column_index_t j = 0; j < i; ++j) {
        // When exception is thrown, i th child is guaranteed unset.
        out.children[j]->release(out.children[j]);
      }
      throw;
    }
  }
}

template <typename Vector>
bool isCompact(const Vector& vec) {
  for (vector_size_t i = 1; i < vec.size(); ++i) {
    if (vec.offsetAt(i - 1) + vec.sizeAt(i - 1) != vec.offsetAt(i)) {
      return false;
    }
  }
  return true;
}

template <typename Vector>
void exportOffsets(
    const Vector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder,
    Selection& childRows) {
  auto offsets = AlignedBuffer::allocate<vector_size_t>(
      checkedPlus<size_t>(out.length, 1), pool);
  auto rawOffsets = offsets->asMutable<vector_size_t>();
  if (!rows.changed() && isCompact(vec)) {
    memcpy(rawOffsets, vec.rawOffsets(), sizeof(vector_size_t) * vec.size());
    rawOffsets[vec.size()] = vec.size() == 0
        ? 0
        : vec.offsetAt(vec.size() - 1) + vec.sizeAt(vec.size() - 1);
  } else {
    childRows.clearAll();
    // j: Index of element we are writing.
    // k: Total size so far.
    vector_size_t j = 0, k = 0;
    rows.apply([&](vector_size_t i) {
      rawOffsets[j++] = k;
      if (!vec.isNullAt(i)) {
        childRows.addRange(vec.offsetAt(i), vec.sizeAt(i));
        k += vec.sizeAt(i);
      }
    });
    VELOX_DCHECK_EQ(j, out.length);
    rawOffsets[j] = k;
  }
  holder.setBuffer(offsets);
}

void exportArrays(
    const ArrayVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder) {
  Selection childRows(vec.elements()->size());
  exportOffsets(vec, rows, out, pool, holder, childRows);
  holder.resizeChildren(1);
  exportBase(
      *vec.elements()->loadedVector(),
      childRows,
      *holder.allocateChild(0),
      pool);
  out.n_children = 1;
  out.children = holder.getChildrenArrays();
}

void exportMaps(
    const MapVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool,
    VeloxToBossBridgeHolder& holder) {
  RowVector child(
      pool,
      ROW({"key", "value"}, {vec.mapKeys()->type(), vec.mapValues()->type()}),
      nullptr,
      vec.mapKeys()->size(),
      {vec.mapKeys(), vec.mapValues()});
  Selection childRows(child.size());
  exportOffsets(vec, rows, out, pool, holder, childRows);
  holder.resizeChildren(1);
  exportBase(child, childRows, *holder.allocateChild(0), pool);
  out.n_children = 1;
  out.children = holder.getChildrenArrays();
}

void exportBase(
    const BaseVector& vec,
    const Selection& rows,
    VeloxArray& out,
    memory::MemoryPool* pool) {
  auto holder = std::make_unique<VeloxToBossBridgeHolder>();
  out.buffers = holder->getBossBuffers();
  out.length = rows.count();
  out.offset = 0;
  switch (vec.encoding()) {
    case VectorEncoding::Simple::FLAT:
      exportFlat(vec, rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::ROW:
      exportRows(*vec.asUnchecked<RowVector>(), rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::ARRAY:
      exportArrays(*vec.asUnchecked<ArrayVector>(), rows, out, pool, *holder);
      break;
    case VectorEncoding::Simple::MAP:
      exportMaps(*vec.asUnchecked<MapVector>(), rows, out, pool, *holder);
      break;
    default:
      VELOX_NYI("{} cannot be exported to Boss yet.", vec.encoding());
  }
  out.private_data = holder.release();
  out.release = bridgeRelease;
}

const int64_t *exportToArray(
        const BaseVector &vec,
        const Selection &rows) {
  switch (vec.typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL:
      return vec.values()->as<int64_t>();
    default: VELOX_NYI(
            "Conversion of FlatVector of {} is not supported yet.",
            vec.typeKind());
  }
}

} // namespace

void exportToBoss(
    const VectorPtr& vector,
    VeloxArray& veloxArray,
    memory::MemoryPool* pool) {
  exportBase(*vector, Selection(vector->size()), veloxArray, pool);
}

TypePtr importFromBossType(BossType &bossType) {
  auto format = bossType;

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
          "Unable to convert '{}' BossType format type to Velox.", format);
}

namespace {
// Optionally, holds shared_ptrs pointing to the VeloxArray object that
// holds the buffer object that describes the VeloxArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr) {}
  explicit BufferViewReleaser(
      std::shared_ptr<VeloxArray> veloxArray)
      : arrayReleaser_(std::move(veloxArray)) {}

  void addRef() const {}
  void release() const {}

 private:
  const std::shared_ptr<VeloxArray> arrayReleaser_;
};

// Wraps a naked pointer using a Velox buffer view, without copying it. Adding a
// dummy releaser as the buffer lifetime is fully controled by the client of the
// API.
BufferPtr wrapInBufferViewAsViewer(const void* buffer, size_t length) {
  static const BufferViewReleaser kViewerReleaser;
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer), length, kViewerReleaser);
}

std::optional<int64_t> optionalNullCount(int64_t value) {
  return value == -1 ? std::nullopt : std::optional<int64_t>(value);
}

// Dispatch based on the type.
template <TypeKind kind>
VectorPtr createFlatVector(
    memory::MemoryPool* pool,
    const TypePtr& type,
    BufferPtr nulls,
    size_t length,
    BufferPtr values,
    int64_t nullCount) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_shared<FlatVector<T>>(
      pool,
      type,
      nulls,
      length,
      values,
      std::vector<BufferPtr>(),
      SimpleVectorStats<T>{},
      std::nullopt,
      optionalNullCount(nullCount));
}

using WrapInBufferViewFunc =
    std::function<BufferPtr(const void* buffer, size_t length)>;

VectorPtr importFromBossImpl(
    BossType bossType,
    VeloxArray& veloxArray,
    memory::MemoryPool* pool);

BufferPtr computeSizes(
    const vector_size_t* offsets,
    int64_t length,
    memory::MemoryPool* pool) {
  auto sizesBuf = AlignedBuffer::allocate<vector_size_t>(length, pool);
  auto sizes = sizesBuf->asMutable<vector_size_t>();
  for (int64_t i = 0; i < length; ++i) {
    // `offsets` here has size length + 1 so i + 1 is valid.
    sizes[i] = offsets[i + 1] - offsets[i];
  }
  return sizesBuf;
}

VectorPtr importFromBossImpl(
    BossType bossType,
    VeloxArray& veloxArray,
    memory::MemoryPool* pool,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_USER_CHECK_NOT_NULL(veloxArray.release, "veloxArray was released.");
  VELOX_USER_CHECK_EQ(
      veloxArray.offset,
      0,
      "Offsets are not supported during boss conversion yet.");
  VELOX_CHECK_GE(
      veloxArray.length, 0, "Array length needs to be non-negative.");

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
      veloxArray.buffers, veloxArray.length * type->cppSizeInBytes());

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createFlatVector,
      type->kind(),
      pool,
      type,
      nulls,
      veloxArray.length,
      values,
      veloxArray.null_count);
}

VectorPtr importFromBossImpl(
        BossType bossType,
        VeloxArray &veloxArray,
        memory::MemoryPool *pool) {
  return importFromBossImpl(
          bossType, veloxArray, pool, wrapInBufferViewAsViewer);
}

} // namespace

VectorPtr importFromBossAsViewer(
        BossType bossType,
        const VeloxArray &veloxArray,
        memory::MemoryPool *pool) {
  return importFromBossImpl(
          bossType,
          const_cast<VeloxArray &>(veloxArray),
          pool);
}

//template<typename T>
const int64_t *exportToBoss(const VectorPtr &vector) {
  return exportToArray(*vector, Selection(vector->size()));
}

} // namespace facebook::velox
