#pragma once

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"

//#define DebugInfo
using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace boss::engines::velox {
    /**
 *     bool = 0, long = 1, double = 2 , ::std::string = 3, Symbol = 4
 */
    enum BossType {
        bBOOL = 0,
        bBIGINT,
        bDOUBLE,
        bSTRING
    };

    struct BossArray {
        // Array data description
        int64_t length;
        const void *buffers;

        // Release callback
        void (*release)(struct BossArray *);
    };

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
        std::string tableName;
        std::vector<std::string> selectedColumns;
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
        std::unordered_map<std::string, TypePtr> fileColumnNamesMap;

        void clear() {
            limit = 0;
            orderBy = false;
            tableName.clear();
            selectedColumns.clear();
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
        }
    };

    class QueryBuilder {
    public:
        QueryBuilder() {
            tableCnt = 0;
        }

        int tableCnt;
        FormExpr curVeloxExpr;
        std::vector<FormExpr> veloxExprList;
        std::unordered_map<std::string, std::string> projNameMap;
        std::unordered_map<std::string, std::string> aggrNameMap;
        std::vector<std::unordered_map<std::string, TypePtr>> columnAliaseList;
        std::shared_ptr<memory::MemoryPool> pool_{memory::getDefaultMemoryPool()};

        void mergeGreaterFilter(FiledFilter input);

        void formatVeloxFilter_Join();

        void getFileColumnNamesMap();

        void reformVeloxExpr();

        core::PlanNodePtr getVeloxPlanBuilder();
    };

    VectorPtr importFromBossAsViewer(BossType bossType, const BossArray &bossArray, memory::MemoryPool *pool);

    std::vector<RowVectorPtr> runQuery(const std::shared_ptr<const core::PlanNode> &planNode,
                                       std::unique_ptr<TaskCursor> &taskCursor);

    BossArray makeBossArray(const void *buffers, int64_t length);

    void printResults(const std::vector<RowVectorPtr> &results);

    RowVectorPtr makeRowVector(std::vector<std::string> childNames,
                               std::vector<VectorPtr> children, memory::MemoryPool *pool);

} // namespace boss::engines::velox
