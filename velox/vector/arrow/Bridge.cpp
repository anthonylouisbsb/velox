/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/vector/arrow/Bridge.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {

namespace {

// TODO: The initial supported conversions only use one buffer for nulls and
// one for value, but we'll need more once we support strings and complex types.
static constexpr size_t kMaxBuffers{2};

// Structure that will hold the buffers needed by ArrowArray. This is opaquely
// carried by ArrowArray.private_data
struct VeloxToArrowBridgeHolder {
  // Holds a shared_ptr to the vector being bridged, to ensure its lifetime.
  VectorPtr vector;

  // Holds the pointers to buffers.
  const void* buffers[kMaxBuffers];
};

// Structure that will hold buffers needed by ArrowSchema. This is opaquely
// carried by ArrowSchema.private_data
struct VeloxToArrowSchemaBridgeHolder {
  // Unfortunately, we need two vectors here since ArrowSchema takes a
  // ArrowSchema** pointer for children (so we can't just cast the
  // vector<unique_ptr<>>), but we also need a member to control the
  // lifetime of the children objects. The following invariable should always
  // hold:
  //   childrenRaw[i] == childrenOwned[i].get()
  std::vector<ArrowSchema*> childrenRaw;
  std::vector<std::unique_ptr<ArrowSchema>> childrenOwned;

  // If the input type is a RowType, we keep the shared_ptr alive so we can set
  // ArrowSchema.name pointer to the internal string that contains the column
  // name.
  RowTypePtr rowType;
};

// Release function for ArrowArray. Arrow standard requires it to recurse down
// to children and dictionary arrays, and set release and private_data to null
// to signal it has been released.
static void bridgeRelease(ArrowArray* arrowArray) {
  if (!arrowArray || !arrowArray->release) {
    return;
  }

  // Recurse down to release children arrays.
  for (int64_t i = 0; i < arrowArray->n_children; ++i) {
    ArrowArray* child = arrowArray->children[i];
    if (child != nullptr && child->release != nullptr) {
      child->release(child);
      VELOX_CHECK_NULL(child->release);
    }
  }

  // Release dictionary.
  ArrowArray* dict = arrowArray->dictionary;
  if (dict != nullptr && dict->release != nullptr) {
    dict->release(dict);
    VELOX_CHECK_NULL(dict->release);
  }

  // Destroy the current holder.
  auto* bridgeHolder =
      static_cast<VeloxToArrowBridgeHolder*>(arrowArray->private_data);
  delete bridgeHolder;

  // Finally, mark the array as released.
  arrowArray->release = nullptr;
  arrowArray->private_data = nullptr;
}

// Release function for ArrowSchema. Arrow standard requires it to recurse down
// to all children, and set release and private_data to null to signal it has
// been released.
static void bridgeSchemaRelease(ArrowSchema* arrowSchema) {
  if (!arrowSchema || !arrowSchema->release) {
    return;
  }

  // Recurse down to release children arrays.
  for (int64_t i = 0; i < arrowSchema->n_children; ++i) {
    ArrowSchema* child = arrowSchema->children[i];
    if (child != nullptr && child->release != nullptr) {
      child->release(child);
      VELOX_CHECK_NULL(child->release);
    }
  }

  // Release dictionary.
  ArrowSchema* dict = arrowSchema->dictionary;
  if (dict != nullptr && dict->release != nullptr) {
    dict->release(dict);
    VELOX_CHECK_NULL(dict->release);
  }

  // Destroy the current holder.
  auto* bridgeHolder =
      static_cast<VeloxToArrowSchemaBridgeHolder*>(arrowSchema->private_data);
  delete bridgeHolder;

  // Finally, mark the array as released.
  arrowSchema->release = nullptr;
  arrowSchema->private_data = nullptr;
}

void exportFlatVector(const VectorPtr& vector, ArrowArray& arrowArray) {
  switch (vector->typeKind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      arrowArray.buffers[1] = vector->valuesAsVoid();
      break;

    default:
      VELOX_NYI(
          "Conversion of FlatVector of {} is not supported yet.",
          vector->typeKind());
  }
}

// Returns the Arrow C data interface format type for a given Velox type.
const char* exportArrowFormatStr(const TypePtr& type) {
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
      return "+L"; // large list
    case TypeKind::MAP:
      return "+m"; // map
    case TypeKind::ROW:
      return "+s"; // struct

    default:
      VELOX_NYI("Unable to map type '{}' to ArrowSchema.", type->kind());
  }
}

} // namespace

void exportToArrow(const VectorPtr& vector, ArrowArray& arrowArray) {
  // Bridge holder is stored in private_data, which is a C-compatible naked
  // pointer. However, since this function can throw (unsupported conversion
  // type, for instance), we temporarily use a unique_ptr to ensure the bridge
  // holder is released in case this function fails.
  //
  // Since this unique_ptr dies with this function and we'll need this bridge
  // alive, the last step in this function is to release this unique_ptr.
  auto bridgeHolder = std::make_unique<VeloxToArrowBridgeHolder>();
  bridgeHolder->vector = vector;
  arrowArray.n_buffers = kMaxBuffers;
  arrowArray.buffers = bridgeHolder->buffers;
  arrowArray.release = bridgeRelease;
  arrowArray.length = vector->size();

  // getNullCount() returns a std::optional. -1 means we don't have the count
  // available yet (and we don't want to count it here).
  arrowArray.null_count = vector->getNullCount().value_or(-1);

  // Velox does not support offset'ed vectors yet.
  arrowArray.offset = 0;

  // Setting up buffer pointers. First one is always nulls.
  arrowArray.buffers[0] = vector->rawNulls();

  // Second buffer is values. Only support flat for now.
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      exportFlatVector(vector, arrowArray);
      break;

    default:
      VELOX_NYI("Only FlatVectors can be exported to Arrow for now.");
      break;
  }

  // TODO: No nested types, strings, or dictionaries for now.
  arrowArray.n_children = 0;
  arrowArray.children = nullptr;
  arrowArray.dictionary = nullptr;

  // We release the unique_ptr since bridgeHolder will now be carried inside
  // ArrowArray.
  arrowArray.private_data = bridgeHolder.release();
}

void exportToArrow(const TypePtr& type, ArrowSchema& arrowSchema) {
  arrowSchema.format = exportArrowFormatStr(type);
  arrowSchema.name = nullptr;

  // No additional metadata or support for dictionaries for now.
  arrowSchema.metadata = nullptr;
  arrowSchema.dictionary = nullptr;

  // All supported types are semantically nullable.
  arrowSchema.flags = ARROW_FLAG_NULLABLE;

  // Allocate private data buffer holder and recurse down to children types.
  auto bridgeHolder = std::make_unique<VeloxToArrowSchemaBridgeHolder>();
  const size_t numChildren = type->size();

  if (numChildren > 0) {
    bridgeHolder->childrenRaw.resize(numChildren);
    bridgeHolder->childrenOwned.resize(numChildren);

    // If this is a RowType, hold the shared_ptr so we can set the
    // ArrowSchema.name pointer to its internal `name` string.
    if (type->kind() == TypeKind::ROW) {
      bridgeHolder->rowType = std::dynamic_pointer_cast<const RowType>(type);
    }

    arrowSchema.children = bridgeHolder->childrenRaw.data();
    arrowSchema.n_children = numChildren;

    for (size_t i = 0; i < numChildren; ++i) {
      // Recurse down the children. We use the same trick of temporarily holding
      // the buffer in a unique_ptr so it doesn't leak if the recursion throws.
      //
      // But this is more nuanced: for types with a list of children (like
      // row/structs), if one of the children throws, we need to make sure we
      // call release() on the children that have already been created before we
      // re-throw the exception back to the client, or memory will leak. This is
      // needed because Arrow doesn't define what the client needs to do if the
      // conversion fails, so we can't expect the client to call the release()
      // method.
      try {
        auto& currentSchema = bridgeHolder->childrenOwned[i];
        currentSchema = std::make_unique<ArrowSchema>();
        exportToArrow(type->childAt(i), *currentSchema);

        if (bridgeHolder->rowType) {
          currentSchema->name = bridgeHolder->rowType->nameOf(i).data();
        }
        arrowSchema.children[i] = currentSchema.get();
      } catch (const VeloxException& e) {
        // Release any children that have already been built before re-throwing
        // the exception back to the client.
        for (size_t j = 0; j < i; ++j) {
          arrowSchema.children[j]->release(arrowSchema.children[j]);
        }
        throw;
      }
    }
  } else {
    arrowSchema.n_children = 0;
    arrowSchema.children = nullptr;
  }

  // Set release callback.
  arrowSchema.release = bridgeSchemaRelease;
  arrowSchema.private_data = bridgeHolder.release();
}

TypePtr importFromArrow(const ArrowSchema& arrowSchema) {
  const char* format = arrowSchema.format;
  VELOX_CHECK_NOT_NULL(format);

  switch (format[0]) {
    case 'b':
      return BOOLEAN();
    case 'c':
      return TINYINT();
    case 's':
      return SMALLINT();
    case 'i':
      return INTEGER();
    case 'l':
      return BIGINT();
    case 'f':
      return REAL();
    case 'g':
      return DOUBLE();

    // Map both utf-8 and large utf-8 string to varchar.
    case 'u':
    case 'U':
      return VARCHAR();

    // Same for binary.
    case 'z':
    case 'Z':
      return VARBINARY();

    case 't': // temporal types.
      // Mapping it to ttn for now.
      if (format[1] == 't' && format[2] == 'n') {
        return TIMESTAMP();
      }
      if (format[1] == 'd' && format[2] == 'D') {
        return DATE();
      }
      break;

    // Complex types.
    case '+': {
      switch (format[1]) {
        // Array/list.
        case 'L':
          VELOX_CHECK_EQ(arrowSchema.n_children, 1);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          return ARRAY(importFromArrow(*arrowSchema.children[0]));

        // Map.
        case 'm':
          VELOX_CHECK_EQ(arrowSchema.n_children, 2);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[0]);
          VELOX_CHECK_NOT_NULL(arrowSchema.children[1]);
          return MAP(
              importFromArrow(*arrowSchema.children[0]),
              importFromArrow(*arrowSchema.children[1]));

        // Struct/rows.
        case 's': {
          // Loop collecting the child types and names.
          std::vector<TypePtr> childTypes;
          std::vector<std::string> childNames;
          childTypes.reserve(arrowSchema.n_children);
          childNames.reserve(arrowSchema.n_children);

          for (size_t i = 0; i < arrowSchema.n_children; ++i) {
            VELOX_CHECK_NOT_NULL(arrowSchema.children[i]);
            childTypes.emplace_back(importFromArrow(*arrowSchema.children[i]));
            childNames.emplace_back(
                arrowSchema.children[i]->name != nullptr
                    ? arrowSchema.children[i]->name
                    : "");
          }
          return ROW(std::move(childNames), std::move(childTypes));
        }

        default:
          break;
      }
    } break;

    default:
      break;
  }
  VELOX_USER_FAIL(
      "Unable to convert '{}' ArrowSchema format type to Velox.", format);
}

namespace {
// Optionally, holds shared_ptrs pointing to the ArrowArray object that
// holds the buffer and the ArrowSchema object that describes the ArrowArray,
// which will be released to signal that we will no longer hold on to the data
// and the shared_ptr deleters should run the release procedures if no one
// else is referencing the objects.
struct BufferViewReleaser {
  BufferViewReleaser() : BufferViewReleaser(nullptr, nullptr) {}
  BufferViewReleaser(
      std::shared_ptr<ArrowSchema> arrowSchema,
      std::shared_ptr<ArrowArray> arrowArray)
      : schemaReleaser_(std::move(arrowSchema)),
        arrayReleaser_(std::move(arrowArray)) {}

  void addRef() const {}
  void release() const {}

 private:
  const std::shared_ptr<ArrowSchema> schemaReleaser_;
  const std::shared_ptr<ArrowArray> arrayReleaser_;
};

// Wraps a naked pointer using a Velox buffer view, without copying it. Adding a
// dummy releaser as the buffer lifetime is fully controled by the client of the
// API.
BufferPtr wrapInBufferViewAsViewer(const void* buffer, size_t length) {
  static const BufferViewReleaser kViewerReleaser;
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer), length, kViewerReleaser);
}

// Wraps a naked pointer using a Velox buffer view, without copying it. This
// buffer view uses shared_ptr to manage reference counting and releasing for
// the ArrowSchema object and the ArrowArray object
BufferPtr wrapInBufferViewAsOwner(
    const void* buffer,
    size_t length,
    std::shared_ptr<ArrowSchema> schemaReleaser,
    std::shared_ptr<ArrowArray> arrayReleaser) {
  return BufferView<BufferViewReleaser>::create(
      static_cast<const uint8_t*>(buffer),
      length,
      {std::move(schemaReleaser), std::move(arrayReleaser)});
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
      cdvi::EMPTY_METADATA,
      std::nullopt,
      nullCount == -1 ? std::nullopt : std::optional<int64_t>(nullCount));
}

using WrapInBufferViewFunc =
    std::function<BufferPtr(const void* buffer, size_t length)>;

VectorPtr importFromArrowImpl(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool,
    WrapInBufferViewFunc wrapInBufferView) {
  VELOX_USER_CHECK_NOT_NULL(arrowSchema.release, "arrowSchema was released.");
  VELOX_USER_CHECK_NOT_NULL(arrowArray.release, "arrowArray was released.");
  VELOX_USER_CHECK_NULL(
      arrowArray.dictionary,
      "Dictionary encoded arrowArrays not supported yet.");
  VELOX_USER_CHECK(
      (arrowArray.n_children == 0) && (arrowArray.children == nullptr),
      "Only flat buffers are supported for now.");
  VELOX_USER_CHECK_EQ(
      arrowArray.offset,
      0,
      "Offsets are not supported during arrow conversion yet.");
  VELOX_CHECK_GE(arrowArray.length, 0, "Array length needs to be positive.");

  // First parse and generate a Velox type.
  auto type = importFromArrow(arrowSchema);
  VELOX_CHECK(
      type->isPrimitiveType(),
      "Only conversion of primitive types is supported for now.");

  // Wrap the nulls buffer into a Velox BufferView (zero-copy). Null buffer size
  // needs to be at least one bit per element.
  BufferPtr nulls = nullptr;

  // If either greater than zero or -1 (unknown).
  if (arrowArray.null_count != 0) {
    VELOX_USER_CHECK_NOT_NULL(
        arrowArray.buffers[0],
        "Nulls buffer can't be null unless null_count is zero.");
    nulls = wrapInBufferView(
        arrowArray.buffers[0], bits::nbytes(arrowArray.length));
  } else {
    VELOX_USER_CHECK_NULL(
        arrowArray.buffers[0],
        "Nulls buffer must be nullptr when null_count is zero.");
  }

  // Wrap the values buffer into a Velox BufferView (also zero-copy).
  VELOX_USER_CHECK_EQ(
      arrowArray.n_buffers,
      2,
      "Expecting two buffers as input "
      "(only simple types supported for now).");
  auto values = wrapInBufferView(
      arrowArray.buffers[1], arrowArray.length * type->cppSizeInBytes());

  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      createFlatVector,
      type->kind(),
      pool,
      type,
      nulls,
      arrowArray.length,
      values,
      arrowArray.null_count);
}
} // namespace

VectorPtr importFromArrowAsViewer(
    const ArrowSchema& arrowSchema,
    const ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  return importFromArrowImpl(
      arrowSchema, arrowArray, pool, wrapInBufferViewAsViewer);
}

VectorPtr importFromArrowAsOwner(
    ArrowSchema& arrowSchema,
    ArrowArray& arrowArray,
    memory::MemoryPool* pool) {
  // This Vector will take over the ownership of `arrowSchema` and `arrowArray`
  // by marking them as released and becoming responsible for calling the
  // release callbacks when use count reaches zero. These ArrowSchema object and
  // ArrowArray object will be co-owned by both the BufferVieweReleaser of the
  // nulls buffer and values buffer.
  std::shared_ptr<ArrowSchema> schemaReleaser(
      new ArrowSchema(arrowSchema), [](ArrowSchema* toDelete) {
        if (toDelete != nullptr) {
          if (toDelete->release != nullptr) {
            toDelete->release(toDelete);
          }
          delete toDelete;
        }
      });
  std::shared_ptr<ArrowArray> arrayReleaser(
      new ArrowArray(arrowArray), [](ArrowArray* toDelete) {
        if (toDelete != nullptr) {
          if (toDelete->release != nullptr) {
            toDelete->release(toDelete);
          }
          delete toDelete;
        }
      });
  VectorPtr imported = importFromArrowImpl(
      arrowSchema,
      arrowArray,
      pool,
      [&schemaReleaser, &arrayReleaser](const void* buffer, size_t length) {
        return wrapInBufferViewAsOwner(
            buffer, length, schemaReleaser, arrayReleaser);
      });

  arrowSchema.release = nullptr;
  arrowArray.release = nullptr;

  return imported;
}

} // namespace facebook::velox
