#pragma once

#include "BridgeVelox.h"

#define SUPPORT_RADIX_JOINS
#define SUPPORT_NEW_NUM_SPLITS

namespace boss::engines::velox {

    enum ColumnType {
        cName, cValue
    };

    struct AtomicExpr {
        enum ColumnType type;
        std::string data;
    };

    struct FiledFilter {
        std::string opName;
        std::vector<AtomicExpr> element;

        void clear() {
            opName.clear();
            element.clear();
        }
    };

    struct JoinPair {
        std::string leftKey;
        std::string rightKey;
    };

    struct aggrPair {
        std::string op;
        std::string oldName;
        std::string newName;

        aggrPair(std::string op_in, std::string oldName_in, std::string newName_in)
                : op(std::move(op_in)), oldName(std::move(oldName_in)), newName(std::move(newName_in)) {}
    };

    struct JoinPairList {
        bool leftFlag = false;
        std::vector<std::string> leftKeys;
        std::vector<std::string> rightKeys;

        void clear() {
            leftFlag = false;
            leftKeys.clear();
            rightKeys.clear();
        }
    };

    struct FormExpr {
        int32_t limit = 0;
        bool orderBy = false;
        bool delayJoinList = false;
        std::string tableName;
        std::vector<std::string> selectedColumns;
        std::vector<std::string> outColumns;
        std::vector<FiledFilter> tmpFieldFiltersVec;
        std::vector<std::string> fieldFiltersVec;
        std::string remainingFilter;
        std::vector<std::string> projectionsVec;
        std::vector<std::string> groupingKeysVec;
        std::vector<aggrPair> aggregatesVec;
        std::vector<std::string> orderByVec;
        std::vector<JoinPair> hashJoinVec;
        std::vector<JoinPairList> hashJoinListVec;
        std::string filter;  // can be used to filter non-field clause
        std::vector<RowVectorPtr> rowDataVec;
        RowTypePtr tableSchema;
        std::unordered_map<std::string, TypePtr> fileColumnNamesMap;
        std::vector<BufferPtr> indicesVec;
        size_t indicesColumnEndIndex;
        std::vector<int64_t> radixPartitions;
        std::vector<size_t> spanRowCountVec;

        void clear() {
            limit = 0;
            orderBy = false;
            delayJoinList = false;
            tableName.clear();
            selectedColumns.clear();
            outColumns.clear();
            tmpFieldFiltersVec.clear();
            fieldFiltersVec.clear();
            remainingFilter.clear();
            projectionsVec.clear();
            groupingKeysVec.clear();
            aggregatesVec.clear();
            orderByVec.clear();
            hashJoinVec.clear();
            hashJoinListVec.clear();
            filter.clear();
            rowDataVec.clear();
            indicesVec.clear();
            radixPartitions.clear();
            spanRowCountVec.clear();
        }
    };

    class QueryBuilder {
    public:
        QueryBuilder(memory::MemoryPool& pool) : tableCnt(0), pool_(pool) {}

        int tableCnt;
        FormExpr curVeloxExpr;
        std::vector<FormExpr> veloxExprList;
        std::unordered_map<std::string, std::string> projNameMap;
        std::unordered_map<std::string, std::string> aggrNameMap;
        std::vector<std::unordered_map<std::string, TypePtr>> columnAliaseList;
        memory::MemoryPool& pool_;
        FiledFilter tmpFieldFilter;
        JoinPairList tmpJoinPairList;

        void mergeGreaterFilter(FiledFilter input);

        void formatVeloxFilter_Join();

        void getFileColumnNamesMap();

        void reformVeloxExpr();

        core::PlanNodePtr getVeloxPlanBuilder(std::vector<core::PlanNodeId> &scanIds);

        void getTableMeta(ComplexExpression &&expression);

        void add_tmpFieldFilter(std::string data, ColumnType type);

        void add_selectedColumns(const std::string &colName);

        void postTransFilter_Join(const std::string &headName);

        void postTransProj_PartialAggr(std::vector<std::string> &projectionList,
                                       std::vector<std::string> lastProjectionsVec,
                                       const std::string &projectionName);

        void postTransSum(const std::string &oldName);
    };

} // namespace boss::engines::velox
