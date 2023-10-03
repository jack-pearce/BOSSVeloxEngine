#include "BossConnector.h"

using namespace facebook::velox::exec::test;

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

//  bossRowData1D_.reserve(spanRowCountAcc_.back());
//  for(const auto& subVec : bossRowDataVec_) {
//    bossRowData1D_.insert(bossRowData1D_.end(), subVec->children().begin(),
//                          subVec->children().end());
//  }

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

#if 0
void BossDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(currentSplit_, nullptr,
                 "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<BossConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for BossDataSource.");

  auto totalRowCount = spanRowCountAcc_.back();
  auto maxTotalParts =
      (currentSplit_->totalParts > totalRowCount) ? totalRowCount : currentSplit_->totalParts;
  // invalid split
  if(currentSplit_->partNumber >= maxTotalParts) {
    splitEnd_ = splitOffset_;
    return;
  }
  size_t partSize = std::ceil((double)totalRowCount / (double)maxTotalParts);

  splitOffset_ = partSize * currentSplit_->partNumber;
  splitEnd_ = splitOffset_ + partSize;
}
#else
void BossDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(currentSplit_, nullptr,
                 "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<BossConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for BossDataSource.");

  if(!firstAddSplit_) {
    spanRowCountAcc.emplace_back(0);
    auto spanSize = bossSpanRowCountVec_.size();
    for(size_t i = 0; i < spanSize; i++) {
      spanRowCountAcc.emplace_back(spanRowCountAcc.back() + bossSpanRowCountVec_.at(i));
    }
    totalRowCount = spanRowCountAcc.back();
    maxTotalParts =
        (currentSplit_->totalParts > totalRowCount) ? totalRowCount : currentSplit_->totalParts;
    partSize = std::ceil((double)totalRowCount / (double)maxTotalParts);
    firstAddSplit_ = true;
  }

  // invalid split
  if(currentSplit_->partNumber >= maxTotalParts) {
    splitEnd_ = splitOffset_;
    return;
  }

  // invalid split
  if(partSize * currentSplit_->partNumber >= totalRowCount) {
    splitEnd_ = splitOffset_;
    return;
  }

  size_t idx = 1;
  while(1) {
    if(partSize * currentSplit_->partNumber < spanRowCountAcc.at(idx)) {
      break;
    }
    idx++;
  }
  spanCountIdx_ = idx - 1;
  splitOffset_ = partSize * currentSplit_->partNumber - spanRowCountAcc.at(spanCountIdx_);

  // start from a new span
  if(splitOffset_ > 0 && splitOffset_ < partSize)
    splitOffset_ = 0;

  splitEnd_ = splitOffset_ + partSize;
  if(splitEnd_ > bossSpanRowCountVec_.at(spanCountIdx_))
    splitEnd_ = bossSpanRowCountVec_.at(spanCountIdx_);

  // remaining rows are all belong to the last split
  if(currentSplit_->partNumber == maxTotalParts - 1) {
    if(splitEnd_ < bossSpanRowCountVec_.at(spanCountIdx_))
      splitEnd_ = bossSpanRowCountVec_.at(spanCountIdx_);
  }
}
#endif

RowVectorPtr BossDataSource::getBossData(uint64_t startOffset, size_t maxRows) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  auto lengthLeft = maxRows;
  auto offset = startOffset;
  auto idx = spanCountIdx_;
//  while(lengthLeft) {
    auto length = lengthLeft;
//    if(offset + length > bossSpanRowCountVec_.at(idx))
//      length = bossSpanRowCountVec_.at(idx) - offset;
    for(const auto channel : outputColumnMappings_) {
      children.emplace_back(bossRowDataVec_.at(idx)->childAt(channel)->slice(offset, length));
    }

//    lengthLeft -= length;
//    offset = 0;
//    idx++;
//  }

  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), maxRows,
                                     std::move(children));
}

RowVectorPtr BossDataSource::getBossData1D(uint64_t startOffset, size_t maxRows) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  if(maxRows > 0) {
      for(const auto channel : outputColumnMappings_) {
      children.emplace_back(bossRowData1D_.at(channel)->slice(startOffset, maxRows));
      }
      return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), maxRows,
                                         std::move(children));
  }

  return nullptr;
}

std::optional<RowVectorPtr> BossDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(currentSplit_, "No split to process. Call addSplit() first.");

  size_t maxRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector = getBossData(splitOffset_, maxRows);
//  auto outputVector = getBossData1D(splitOffset_, maxRows);

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
