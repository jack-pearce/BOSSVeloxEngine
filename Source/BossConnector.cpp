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
  VELOX_CHECK_EQ(currentSplit_, nullptr,
                 "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<BossConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for BossDataSource.");

  if(!firstAddSplit_) {
    totalParts_ = bossSpanRowCountVec_.size();
    while(1) {
      if(totalParts_ * 2 > currentSplit_->totalParts) {
        break;
      }
      totalParts_ *= 2;
    }

    subParts_ = totalParts_ / bossSpanRowCountVec_.size();
    partSize_ = std::ceil((double)bossSpanRowCountVec_.at(0) / (double)subParts_);
    if(subParts_ > bossSpanRowCountVec_.at(0)) {
      partSize_ = 1;
      totalParts_ = std::accumulate(bossSpanRowCountVec_.begin(), bossSpanRowCountVec_.end(), 0);
    }
//    std::cout << "totalParts_ " << totalParts_ << '\n';
    firstAddSplit_ = true;
  }

  // invalid split
  if(currentSplit_->partNumber >= totalParts_) {
    spanCountIdx_ = 0;
    splitEnd_ = splitOffset_;
    return;
  }

  spanCountIdx_ = currentSplit_->partNumber / subParts_;
  assert(spanCountIdx_ < bossSpanRowCountVec_.size());
  auto subPartIdx = currentSplit_->partNumber % subParts_;
  splitOffset_ = subPartIdx * partSize_;
  splitEnd_ = splitOffset_ + partSize_;
  if(splitEnd_ > bossSpanRowCountVec_.at(spanCountIdx_)) {
    splitEnd_ = bossSpanRowCountVec_.at(spanCountIdx_);
  }
}

RowVectorPtr BossDataSource::getBossData(uint64_t length) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  for(const auto channel : outputColumnMappings_) {
    children.emplace_back(
        bossRowDataVec_.at(spanCountIdx_)->childAt(channel)->slice(splitOffset_, length));
  }

  return std::make_shared<RowVector>(pool_, outputType_, BufferPtr(nullptr), length,
                                     std::move(children));
}

std::optional<RowVectorPtr> BossDataSource::next(uint64_t size, ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(currentSplit_, "No split to process. Call addSplit() first.");

  auto maxRows = std::min(size, (splitEnd_ - splitOffset_));
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
