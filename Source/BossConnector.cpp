#include "BossConnector.h"

#ifdef DebugInfo
  #include <iostream>
#endif // DebugInfo

namespace boss::engines::velox {

std::string BossTableHandle::toString() const {
  return fmt::format("table: {}, spanRowCount size: {}", tableName_, spanRowCountVec_.size());
}

BossDataSource::BossDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool) {
  auto bossTableHandle = std::dynamic_pointer_cast<BossTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(bossTableHandle, "TableHandle must be an instance of BossTableHandle");
  bossTableName_ = bossTableHandle->getTable();
  bossRowDataVec_ = bossTableHandle->getRowDataVec();
  bossSpanRowCountVec_ = bossTableHandle->getSpanRowCountVec();

  auto bossTableSchema = bossTableHandle->getTableSchema();
  VELOX_CHECK_NOT_NULL(bossTableSchema, "BossSchema can't be null.");
  
  outputColumnMappings_.reserve(outputType->size());

  for(const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(it != columnHandles.end(),
                "ColumnHandle is missing for output column '{}' on table '{}'", outputName,
                bossTableName_);

    auto handle = std::dynamic_pointer_cast<BossColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(handle,
                         "ColumnHandle must be an instance of BossColumnHandle "
                         "for '{}' on table '{}'",
                         handle->name(), bossTableName_);

    auto idx = bossTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx != std::nullopt,
        "Column '{}' not found on TPC-H table '{}'.",
        handle->name(),
        bossTableName_);
    outputColumnMappings_.emplace_back(*idx);
  }
  outputType_ = outputType;
}

void BossDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(currentSplit_,
                 "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<BossConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(currentSplit_, "Wrong type of split for BossDataSource.");

  if(!firstAddSplit_) {
#ifdef SUPPORT_NEW_NUM_SPLITS
    if(currentSplit_->totalParts < bossSpanRowCountVec_.size()) {
      totalParts_ = bossSpanRowCountVec_.size();
    } else {
      totalParts_ = currentSplit_->totalParts;
      totalParts_ -= totalParts_ % bossSpanRowCountVec_.size();
    }
#else
    totalParts_ = bossSpanRowCountVec_.size();
    while(1) {
      if(totalParts_ * 2 > currentSplit_->totalParts) {
        break;
      }
      totalParts_ *= 2;
    }
#endif // SUPPORT_NEW_NUM_SPLITS

    subParts_ = totalParts_ / bossSpanRowCountVec_.size();
    partSize_ = std::ceil((double)bossSpanRowCountVec_.at(0) / (double)subParts_);
    if(subParts_ > bossSpanRowCountVec_.at(0)) {
      partSize_ = 1;
      totalParts_ = std::accumulate(bossSpanRowCountVec_.begin(), bossSpanRowCountVec_.end(), 0);
      subParts_ = bossSpanRowCountVec_.at(0);
    }
#ifdef DebugInfo
    std::cout << "bossSpanRowCountVec_.size() " << bossSpanRowCountVec_.size() << std::endl;
    std::cout << "totalParts_ " << totalParts_ << std::endl;
    std::cout << "subParts_ " << subParts_ << std::endl;
    std::cout << "partSize_ " << partSize_ << std::endl;
#endif // DebugInfo
    firstAddSplit_ = true;
  }

  // invalid split
  if(currentSplit_->partNumber >= totalParts_) {
    spanCountIdx_ = 0;
    splitOffset_ = 0;
    splitEnd_ = 0;
    return;
  }

  spanCountIdx_ = currentSplit_->partNumber / subParts_;
  assert(spanCountIdx_ < bossSpanRowCountVec_.size());
  auto subPartIdx = currentSplit_->partNumber % subParts_;
  splitOffset_ = subPartIdx * partSize_;
  // invalid split
  if(splitOffset_ >= bossSpanRowCountVec_.at(spanCountIdx_)) {
    spanCountIdx_ = 0;
    splitOffset_ = 0;
    splitEnd_ = 0;
    return;
  }
  splitEnd_ = splitOffset_ + partSize_;
  if(splitEnd_ > bossSpanRowCountVec_.at(spanCountIdx_)) {
    splitEnd_ = bossSpanRowCountVec_.at(spanCountIdx_);
  }
}

RowVectorPtr BossDataSource::getBossData(uint64_t length) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  //std::cout << "getBossData: spanCountIdx_=" << spanCountIdx_ << " splitOffset_=" << splitOffset_
  //          << " length=" << length << std::endl;

  assert(splitOffset_ <= INT_MAX);
  assert(length <= INT_MAX);
  for(const auto channel : outputColumnMappings_) {
    children.emplace_back(
        bossRowDataVec_.at(spanCountIdx_)->childAt(channel)->slice(splitOffset_, length));
  }

  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), length,
                                     std::move(children));
}

std::optional<RowVectorPtr> BossDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(currentSplit_, "No split to process. Call addSplit() first.");

  auto maxRows = splitEnd_ - splitOffset_;
  auto outputVector = getBossData(maxRows);

  // If the split is exhausted.
  if(!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return std::move(outputVector);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<BossConnectorFactory>())

} // namespace boss::engines::velox
