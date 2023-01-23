#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

/**
 *     bool = 0, long = 1, double = 2 , ::std::string = 3, Symbol = 4 , ComplexExpression = 5
 */
enum BossType {
    bBOOL = 0,
    bBIGINT,
    bDOUBLE,
    bSTRING,
    bDATE
};

/// These 2 definitions should be included by user from either
struct BossSchema {
    // Array type description
    const char* name;
    const char* metadata;
    int format;
    int64_t n_children;
    struct BossSchema** children;

    // Release callback
    void (*release)(struct BossSchema*);
    // Opaque producer-specific data
    void* private_data;
};

struct VeloxArray {
    // Array data description
    int64_t length;
    int64_t null_count = 0;
    int64_t offset;
    int64_t n_children;
    const void* buffers;
    struct VeloxArray** children;

    // Release callback
    void (*release)(struct VeloxArray*);
    // Opaque producer-specific data
    void* private_data;
};

namespace facebook::velox {

/// Export a generic Velox Vector to an VeloxArray, as defined by Boss's C data
///
/// The output VeloxArray needs to be allocated by the consumer (either in the
/// heap or stack), and after usage, the standard REQUIRES the client to call
/// the release() function (or memory will leak).
///
/// After exporting, the VeloxArray will hold ownership to the underlying Vector
/// being referenced, so the consumer does not need to explicitly hold on to the
/// input Vector shared_ptr.
///
/// The function takes a memory pool where allocations will be made (in cases
/// where the conversion is not zero-copy, e.g. for strings) and throws in case
/// the conversion is not implemented yet.
///
/// Example usage:
///
///   VeloxArray veloxArray;
///   exportToBoss(inputVector, veloxArray);
///   inputVector.reset(); // don't need to hold on to this shared_ptr.
///
///   (use veloxArray)
///
///   veloxArray.release(&veloxArray);
///
void exportToBoss(
    const VectorPtr& vector,
    VeloxArray& veloxArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Import an VeloxArray and BossType into a Velox vector.
///
/// This function takes both an VeloxArray (which contains the buffers) and an
/// BossType (which describes the data type), since a Velox vector needs
/// both (buffer and type). A memory pool is also required, since all vectors
/// carry a pointer to it, but not really used in most cases - unless the
/// conversion itself requires a new allocation. In most cases no new
/// allocations are required, unless for arrays of varchars (or varbinaries) and
/// complex types written out of order.
///
/// The new Velox vector returned contains only references to the underlying
/// buffers, so it's the client's responsibility to ensure the buffer's
/// lifetime.
///
/// The function throws in case the conversion fails.
///
/// Example usage:
///
///   BossType bossType;
///   VeloxArray veloxArray;
///   ... // fills structures
///   auto vector = importFromBossAsViewer(bossType, veloxArray, pool);
///   ... // ensure buffers in veloxArray remain alive while vector is used.
///
VectorPtr importFromBossAsViewer(
    BossType bossType,
    const VeloxArray& veloxArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

//template<typename T>
const int64_t *exportToBoss(const VectorPtr &vector);

} // namespace facebook::velox
