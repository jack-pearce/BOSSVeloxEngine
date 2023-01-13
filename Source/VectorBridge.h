#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/vector/BaseVector.h"

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

struct BossArray {
    // Array data description
    int64_t length;
    int64_t null_count = 0;
    int64_t offset;
    int64_t n_children;
    const void* buffers;
    struct BossArray** children;

    // Release callback
    void (*release)(struct BossArray*);
    // Opaque producer-specific data
    void* private_data;
};

namespace facebook::velox {

/// Export a generic Velox Vector to an BossArray, as defined by Boss's C data
///
/// The output BossArray needs to be allocated by the consumer (either in the
/// heap or stack), and after usage, the standard REQUIRES the client to call
/// the release() function (or memory will leak).
///
/// After exporting, the BossArray will hold ownership to the underlying Vector
/// being referenced, so the consumer does not need to explicitly hold on to the
/// input Vector shared_ptr.
///
/// The function takes a memory pool where allocations will be made (in cases
/// where the conversion is not zero-copy, e.g. for strings) and throws in case
/// the conversion is not implemented yet.
///
/// Example usage:
///
///   BossArray bossArray;
///   exportToBoss(inputVector, bossArray);
///   inputVector.reset(); // don't need to hold on to this shared_ptr.
///
///   (use bossArray)
///
///   bossArray.release(&bossArray);
///
void exportToBoss(
    const VectorPtr& vector,
    BossArray& bossArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

/// Export the type of a Velox vector to an BossSchema.
///
/// The guidelines on API usage and memory management are the same as the ones
/// described for the VectorPtr->BossArray export function.
///
/// The function throws in case there was no valid conversion available.
///
/// Example usage:
///
///   BossSchema bossSchema;
///   exportToBoss(inputType, bossSchema);
///   inputType.reset(); // don't need to hold on to this shared_ptr.
///
///   (use bossSchema)
///
///   bossSchema.release(&bossSchema);
///
/// NOTE: Since Boss couples type and encoding, we need both Velox type and
/// actual data (containing encoding) to create an BossSchema.
void exportToBoss(const VectorPtr&, BossSchema&);

/// Import an BossSchema into a Velox Type object.
///
/// This function does the exact opposite of the function above. TypePtr carries
/// all buffers they need to represent types, so after this function returns,
/// the client is free to release any buffers associated with the input
/// BossSchema object.
///
/// The function throws in case there was no valid conversion available.
///
/// Example usage:
///
///   BossSchema bossSchema;
///   ... // fills bossSchema
///   auto type = importFromBoss(bossSchema);
///
///   bossSchema.release(&bossSchema);
///
TypePtr importFromBoss(const BossSchema& bossSchema);

/// Import an BossArray and BossSchema into a Velox vector.
///
/// This function takes both an BossArray (which contains the buffers) and an
/// BossSchema (which describes the data type), since a Velox vector needs
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
///   BossSchema bossSchema;
///   BossArray bossArray;
///   ... // fills structures
///   auto vector = importFromBoss(bossSchema, bossArray, pool);
///   ... // ensure buffers in bossArray remain alive while vector is used.
///
VectorPtr importFromBossAsViewer(
    const BossSchema& bossSchema,
    const BossArray& bossArray,
    memory::MemoryPool* pool =
        &velox::memory::getProcessDefaultMemoryManager().getRoot());

//template<typename T>
const int64_t *exportToBoss(const VectorPtr &vector);

} // namespace facebook::velox
