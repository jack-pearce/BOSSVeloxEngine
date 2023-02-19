#include "BOSSQueryBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

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

    /// Run a given planNode
    /// \param planNode
    /// \param [out] taskCursor a reference to the execution context.
    /// \return list of  Row vector
    RowVectorPtr runQuery(const std::shared_ptr<const core::PlanNode> &planNode,
                          std::unique_ptr<TaskCursor> &taskCursor) {
        CursorParameters params;
        params.planNode = planNode;

        taskCursor = std::make_unique<TaskCursor>(params);

        std::vector<RowVectorPtr> actualResults;
        while (taskCursor->moveNext()) {
            actualResults.push_back(taskCursor->current());
        };
        return actualResults[0];
    }

    int64_t toDate(std::string_view stringDate) {
        Date date;
        parseTo(stringDate, date);
        return date.days();
    }

    void printResults(const std::vector<RowVectorPtr> &results) {
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

    std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};

    RowVectorPtr makeRowVector(std::vector<std::string> childNames, std::vector<VectorPtr> children) {
        std::vector<std::shared_ptr<const Type>> childTypes;
        childTypes.resize(children.size());
        for (int i = 0; i < children.size(); i++) {
            childTypes[i] = children[i]->type();
        }
        auto rowType = ROW(std::move(childNames), std::move(childTypes));
        const size_t vectorSize = children.empty() ? 0 : children.front()->size();

        return std::make_shared<RowVector>(pool_.get(), rowType, BufferPtr(nullptr), vectorSize,
                                           children);
    }

/// DWRF does not support Date type and Varchar is used.
/// Return the Date filter expression as per data format.
    std::string formatDateFilter(
            const std::string &stringDate,
            const RowTypePtr &rowType,
            const std::string &lowerBound,
            const std::string &upperBound) {
        bool isDwrf = rowType->findChild(stringDate)->isVarchar();
        auto suffix = isDwrf ? "" : "::DATE";

        if (!lowerBound.empty() && !upperBound.empty()) {
            return fmt::format(
                    "{} between {}{} and {}{}",
                    stringDate,
                    lowerBound,
                    suffix,
                    upperBound,
                    suffix);
        } else if (!lowerBound.empty()) {
            return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
        } else if (!upperBound.empty()) {
            return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
        }

        VELOX_FAIL(
                "Date range check expression must have either a lower or an upper bound");
    }

    std::vector<std::string> mergeColumnNames(
            const std::vector<std::string> &firstColumnVector,
            const std::vector<std::string> &secondColumnVector) {
        std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
        mergedColumnVector.insert(
                mergedColumnVector.end(),
                secondColumnVector.begin(),
                secondColumnVector.end());
        return mergedColumnVector;
    };

    std::vector<std::unordered_map<std::string, TypePtr>> getFileColumnNamesMap(
            std::vector<FormExpr> &veloxExprList) {
        std::vector<std::unordered_map<std::string, TypePtr>> columnAliaseList;
        for (int i = 0; i < veloxExprList.size(); i++) {
            columnAliaseList.emplace_back(veloxExprList[i].fileColumnNamesMap);
        }
        return columnAliaseList;
    }

    RowTypePtr getOutputRowInfo(std::unordered_map<std::string, TypePtr> fileColumnNamesMap,
                                const std::vector<std::string> &columnNames) {
        std::vector<std::string> names;
        std::vector<TypePtr> types;
        for (auto &columnName: columnNames) {
            names.push_back(columnName);
            types.push_back(fileColumnNamesMap.at(columnName));
        }
        return std::make_shared<RowType>(std::move(names), std::move(types));
    }

    core::PlanNodePtr getVeloxPlanBuilder(std::vector<FormExpr> veloxExprList,
                                          std::vector<std::unordered_map<std::string, TypePtr>> columnAliaseList) {
        auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
        std::vector<core::PlanNodePtr> tableMapPlan;
        std::unordered_map<std::string, int> joinMapPlan;
        std::vector<std::string> outputLayout;
        for (auto itExpr = veloxExprList.begin(); itExpr != veloxExprList.end(); ++itExpr) {
            auto &veloxExpr = *itExpr;
            const auto &fileColumnNames = veloxExpr.fileColumnNamesMap;

            auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({veloxExpr.rowData});

            if (veloxExpr.fieldFiltersVec.size() > 0) {
                auto filtersCnt = 0;
                while (filtersCnt < veloxExpr.fieldFiltersVec.size()) {
                    plan.filter(veloxExpr.fieldFiltersVec[filtersCnt++]);
                }
            }
            // list join first
            if (!veloxExpr.hashJoinListVec.empty()) {
                for (auto itJoin = veloxExpr.hashJoinListVec.begin();
                     itJoin != veloxExpr.hashJoinListVec.end(); ++itJoin) {
                    auto const &hashJoinPair = *itJoin;
                    auto leftKey = hashJoinPair.leftKeys[0];
                    auto rightKey = hashJoinPair.rightKeys[0];
                    auto idxLeft = fileColumnNames.find(leftKey);
                    auto idxRight = fileColumnNames.find(rightKey);
                    if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                        int tableIdx; // find right key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                tableIdx = j;
                                break;
                            }
                        }
                        auto tableName = veloxExpr.tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableIdx].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableIdx];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableIdx];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.leftKeys},
                                {hashJoinPair.rightKeys},
                                build,
                                "",
                                outputLayout);
                    } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                        int tableIdx; // find left key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                tableIdx = j;
                                break;
                            }
                        }
                        auto tableName = veloxExpr.tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableIdx].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableIdx];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableIdx];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.rightKeys},
                                {hashJoinPair.leftKeys},
                                build,
                                "",
                                outputLayout);
                    } else {  // both left and right key are not in the current table
                        int tableLeft; // find left key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                tableLeft = j;
                                break;
                            }
                        }
                        int tableRight; // find right key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                tableRight = j;
                                break;
                            }
                        }
                        auto tableName = veloxExprList[tableRight].tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableLeft].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableLeft];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableLeft];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.rightKeys},
                                {hashJoinPair.leftKeys},
                                build,
                                "",
                                outputLayout);
                    }
                }
            }

            if (!veloxExpr.hashJoinVec.empty()) {
                for (auto itJoin = veloxExpr.hashJoinVec.begin(); itJoin != veloxExpr.hashJoinVec.end(); ++itJoin) {
                    auto const &hashJoinPair = *itJoin;
                    auto idxLeft = fileColumnNames.find(hashJoinPair.leftKey);
                    auto idxRight = fileColumnNames.find(hashJoinPair.rightKey);
                    if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                        int tableIdx; // find right key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                tableIdx = j;
                                break;
                            }
                        }
                        auto tableName = veloxExpr.tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableIdx].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableIdx];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableIdx];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.leftKey},
                                {hashJoinPair.rightKey},
                                build,
                                "",
                                outputLayout);
                    } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                        int tableIdx; // find left key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                tableIdx = j;
                                break;
                            }
                        }
                        auto tableName = veloxExpr.tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableIdx].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableIdx];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableIdx];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.rightKey},
                                {hashJoinPair.leftKey},
                                build,
                                "",
                                outputLayout);
                    } else {  // both left and right key are not in the current table
                        int tableLeft; // find left key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                tableLeft = j;
                                break;
                            }
                        }
                        int tableRight; // find right key table
                        for (int j = 0; j < columnAliaseList.size(); j++) {
                            if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                tableRight = j;
                                break;
                            }
                        }
                        auto tableName = veloxExprList[tableRight].tableName;
                        auto it = joinMapPlan.find(tableName);
                        if (it == joinMapPlan.end()) {
                            joinMapPlan.emplace(tableName, 0xff);
                            outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                        }
                        tableName = veloxExprList[tableLeft].tableName;
                        it = joinMapPlan.find(tableName);
                        core::PlanNodePtr build;
                        if (it == joinMapPlan.end()) {  // first time
                            joinMapPlan.emplace(tableName, 0xff);
                            build = tableMapPlan[tableLeft];
                            outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                        } else {
                            if (it->second == 0xff)
                                build = tableMapPlan[tableLeft];
                            else
                                build = tableMapPlan[it->second];
                            joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                        }
                        plan.hashJoin(
                                {hashJoinPair.rightKey},
                                {hashJoinPair.leftKey},
                                build,
                                "",
                                outputLayout);
                    }
                }
            }
            if (!veloxExpr.projectionsVec.empty()) {
                plan.project(veloxExpr.projectionsVec);
            }
            if (!veloxExpr.groupingKeysVec.empty() || !veloxExpr.aggregatesVec.empty()) {
                std::vector<std::string> aggregatesVec;
                veloxExpr.selectedColumns = veloxExpr.groupingKeysVec;
                for (auto itAggr = veloxExpr.aggregatesVec.begin(); itAggr != veloxExpr.aggregatesVec.end(); ++itAggr) {
                    auto aggregation = *itAggr;
                    auto tmp = fmt::format("{}({}) as {}", aggregation.op, aggregation.oldName, aggregation.newName);
                    aggregatesVec.emplace_back(tmp);
                    veloxExpr.selectedColumns.emplace_back(aggregation.newName);
                }
                plan.partialAggregation(veloxExpr.groupingKeysVec, aggregatesVec);
            }
            if ((itExpr == veloxExprList.end() - 1) &&
                (!veloxExpr.groupingKeysVec.empty() || !veloxExpr.aggregatesVec.empty())) {
                plan.localPartition({});
                plan.finalAggregation();
            }
            if (!veloxExpr.orderByVec.empty()) {
                plan.orderBy(veloxExpr.orderByVec, false);
            }
            if (!veloxExpr.filter.empty()) {
                plan.filter(veloxExpr.filter);
            }
            if (veloxExpr.limit > 0) {
                plan.limit(0, veloxExpr.limit, false);
            }
            auto planPtr = plan.planNode();

            outputLayout = planPtr->outputType()->names();
            tableMapPlan.push_back(std::move(planPtr));
        }
        std::cout << "VeloxPlanBuilder Finished." << std::endl;
        return tableMapPlan.back();
    }

} // namespace boss::engines::velox
