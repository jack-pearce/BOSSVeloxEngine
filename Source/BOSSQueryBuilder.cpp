#include "BOSSQueryBuilder.h"

namespace boss::engines::velox {

    std::vector<std::string> mergeColumnNames(
            const std::vector<std::string> &firstColumnVector,
            const std::vector<std::string> &secondColumnVector) {
        std::vector<std::string> mergedColumnVector = firstColumnVector;
        mergedColumnVector.insert(
                mergedColumnVector.end(),
                secondColumnVector.begin(),
                secondColumnVector.end());
        return mergedColumnVector;
    }

    void QueryBuilder::mergeGreaterFilter(FiledFilter input) {
        if (input.element[0].type == cName) {
            auto colName = input.element[0].data;
            for (int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
                if (curVeloxExpr.tmpFieldFiltersVec[i].element[0].type == cValue &&
                    curVeloxExpr.tmpFieldFiltersVec[i].element[1].type == cName &&
                    curVeloxExpr.tmpFieldFiltersVec[i].element[1].data == colName) {
                    input.element.push_back(curVeloxExpr.tmpFieldFiltersVec[i].element[0]);
                    input.opName = "Between";
                    curVeloxExpr.tmpFieldFiltersVec[i] = input;
                    return;
                }
            }
            curVeloxExpr.tmpFieldFiltersVec.push_back(input);
        } else if (input.element[1].type == cName) {
            auto colName = input.element[1].data;
            for (int i = 0; i < curVeloxExpr.tmpFieldFiltersVec.size(); i++) {
                if (curVeloxExpr.tmpFieldFiltersVec[i].element[0].type == cName &&
                    curVeloxExpr.tmpFieldFiltersVec[i].element[1].type == cValue &&
                    curVeloxExpr.tmpFieldFiltersVec[i].element[0].data == colName) {
                    curVeloxExpr.tmpFieldFiltersVec[i].opName = "Between";
                    curVeloxExpr.tmpFieldFiltersVec[i].element.push_back(input.element[0]);
                    return;
                }
            }
            curVeloxExpr.tmpFieldFiltersVec.push_back(input);
        }
    }

    void QueryBuilder::formatVeloxFilter_Join() {
        curVeloxExpr.fieldFiltersVec.clear();
        curVeloxExpr.hashJoinVec.clear();
        if (curVeloxExpr.tableName == "tmp") {
            for (auto &filter: curVeloxExpr.tmpFieldFiltersVec) {
                if (filter.opName == "Greater") {
                    if (filter.element[0].type == cName && filter.element[1].type == cValue) {
                        auto tmp = fmt::format("{} > {}", filter.element[0].data,
                                               filter.element[1].data); // the column name should be on the left side.
                        curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                    } else if (filter.element[1].type == cName && filter.element[0].type == cValue) {
                        auto tmp = fmt::format("{} < {}", filter.element[1].data, filter.element[0].data);
                        curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                    }
                } else if (filter.opName == "Between") {
                    auto tmp = fmt::format("{} between {} and {}", filter.element[0].data, filter.element[1].data,
                                           filter.element[2].data);
                    curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                } else if (filter.opName == "Equal") {
                    if (filter.element[0].type == cName && filter.element[1].type == cName) {
                        JoinPair tmpJoinPair;
                        tmpJoinPair.leftKey = filter.element[0].data;
                        tmpJoinPair.rightKey = filter.element[1].data;
                        curVeloxExpr.hashJoinVec.push_back(tmpJoinPair);
                    } else {
                        auto tmp = fmt::format("{} = {}", filter.element[0].data, filter.element[1].data);
                        curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                    }
                } else if (filter.opName == "StringContainsQ") {
                    auto length = filter.element[1].data.size();
                    auto tmp = fmt::format("{} like '%{}%'", filter.element[0].data,
                                           filter.element[1].data.substr(1, length - 2));
                    curVeloxExpr.remainingFilter = std::move(tmp);
                } else VELOX_FAIL("unexpected Filter type")
            }
        } else {
            const auto fileColumnNames = curVeloxExpr.fileColumnNamesMap;
            for (auto &filter: curVeloxExpr.tmpFieldFiltersVec) {
                if (filter.opName == "Greater") {
                    if (filter.element[0].type == cName && filter.element[1].type == cValue) {
                        auto tmp = fmt::format("{} > {}", filter.element[0].data,
                                               filter.element[1].data); // the column name should be on the left side.
                        if (fileColumnNames.find(filter.element[0].data) == fileColumnNames.end()) {
                            curVeloxExpr.filter = std::move(tmp);  // not belong to any field
                        } else {
                            curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                        }
                    } else if (filter.element[1].type == cName && filter.element[0].type == cValue) {
                        auto tmp = fmt::format("{} < {}", filter.element[1].data, filter.element[0].data);
                        if (fileColumnNames.find(filter.element[1].data) == fileColumnNames.end()) {
                            curVeloxExpr.filter = std::move(tmp);
                        } else {
                            curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                        }
                    }
                } else if (filter.opName == "Between") {
                    auto tmp = fmt::format("{} between {} and {}", filter.element[0].data, filter.element[1].data,
                                           filter.element[2].data);
                    curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                } else if (filter.opName == "Equal") {
                    if (filter.element[0].type == cName && filter.element[1].type == cName) {
                        JoinPair tmpJoinPair;
                        tmpJoinPair.leftKey = filter.element[0].data;
                        tmpJoinPair.rightKey = filter.element[1].data;
                        curVeloxExpr.hashJoinVec.push_back(tmpJoinPair);
                    } else {
                        auto tmp = fmt::format("{} = {}", filter.element[0].data, filter.element[1].data);
                        curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
                    }
                } else if (filter.opName == "StringContainsQ") {
                    auto length = filter.element[1].data.size();
                    auto tmp = fmt::format("{} like '%{}%'", filter.element[0].data,
                                           filter.element[1].data.substr(1, length - 2));
                    curVeloxExpr.remainingFilter = std::move(tmp);
                } else VELOX_FAIL("unexpected Filter type")
            }
        }
    }

    void QueryBuilder::getFileColumnNamesMap() {
        veloxExprList.push_back(curVeloxExpr); //push back the last expression to the vector
        for (int i = 0; i < veloxExprList.size(); i++) {
            columnAliaseList.emplace_back(veloxExprList[i].fileColumnNamesMap);
        }
    }

    void QueryBuilder::reformVeloxExpr() {
        for (int i = 0; i < veloxExprList.size(); i++) {
            for (auto it = veloxExprList[i].selectedColumns.begin(); it != veloxExprList[i].selectedColumns.end();) {
                auto name = *it;
                bool resizeFlag = false;
                for (int j = 0; j < columnAliaseList.size(); j++) {
                    auto idx = columnAliaseList[j].find(name);
                    if (idx != columnAliaseList[j].end() &&
                        j != i) { // remove column name in the wrong table planBuilder
                        if (std::find(veloxExprList[j].selectedColumns.begin(), veloxExprList[j].selectedColumns.end(),
                                      name) ==
                            veloxExprList[j].selectedColumns.end()) {
                            veloxExprList[j].selectedColumns.push_back(name);
                        }
                        it = veloxExprList[i].selectedColumns.erase(it);
                        resizeFlag = true;
                        break;
                    }
                    if (idx == columnAliaseList[j].end() && j == i) { // not belong to any table, just rename
                        it = veloxExprList[i].selectedColumns.erase(it);
                        resizeFlag = true;
                        break;
                    }
                }
                if (!resizeFlag) {
                    ++it;
                }
            }
        }
    }

    std::shared_ptr<memory::MemoryPool> QueryBuilder::pool_ = memory::getDefaultMemoryPool();

    core::PlanNodePtr QueryBuilder::getVeloxPlanBuilder() {
        const long MAX_JOIN_WAY = 0xff;
        auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
        std::vector<core::PlanNodePtr> tableMapPlan;
        std::unordered_map<std::string, long> joinMapPlan;
        std::vector<std::string> outputLayout;
        for (auto itExpr = veloxExprList.begin(); itExpr != veloxExprList.end(); ++itExpr) {
            auto &veloxExpr = *itExpr;
            const auto &fileColumnNames = veloxExpr.fileColumnNamesMap;

            auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(veloxExpr.rowDataVec);

            // nothing happened for a table, projection for all columns
            if (veloxExpr.selectedColumns.empty()) {
                assert(veloxExpr.projectionsVec.empty());
                std::for_each(fileColumnNames.begin(),
                              fileColumnNames.end(),
                              [&veloxExpr](auto &&p) {
                                  veloxExpr.selectedColumns.push_back(p.first);
                              });
                veloxExpr.projectionsVec = veloxExpr.selectedColumns;
            }
            //project involved columns only
            plan.project(veloxExpr.selectedColumns);

            if (!veloxExpr.fieldFiltersVec.empty()) {
                auto filtersCnt = 0;
                while (filtersCnt < veloxExpr.fieldFiltersVec.size()) {
                    plan.filter(veloxExpr.fieldFiltersVec[filtersCnt++]);
                }
            }

            // list join first
            if (!veloxExpr.delayJoinList) {
                if (!veloxExpr.hashJoinListVec.empty()) {
                    for (auto itJoin = veloxExpr.hashJoinListVec.begin();
                         itJoin != veloxExpr.hashJoinListVec.end(); ++itJoin) {
                        auto const &hashJoinPair = *itJoin;
                        auto leftKey = hashJoinPair.leftKeys[0];
                        auto rightKey = hashJoinPair.rightKeys[0];
                        auto idxLeft = fileColumnNames.find(leftKey);
                        auto idxRight = fileColumnNames.find(rightKey);
                        if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                            int tableIdx = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.leftKeys},
                                    {hashJoinPair.rightKeys},
                                    build,
                                    "",
                                    outputLayout);
                        } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                            int tableIdx = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.rightKeys},
                                    {hashJoinPair.leftKeys},
                                    build,
                                    "",
                                    outputLayout);
                        } else {  // both left and right key are not in the current table
                            int tableLeft = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                    tableLeft = j;
                                    break;
                                }
                            }
                            int tableRight = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                    tableRight = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExprList[tableRight].tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableLeft].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableLeft];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableLeft];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
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
                            int tableIdx = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.leftKey},
                                    {hashJoinPair.rightKey},
                                    build,
                                    "",
                                    outputLayout);
                        } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                            int tableIdx = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.rightKey},
                                    {hashJoinPair.leftKey},
                                    build,
                                    "",
                                    outputLayout);
                        } else {  // both left and right key are not in the current table
                            int tableLeft = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                    tableLeft = j;
                                    break;
                                }
                            }
                            int tableRight = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                    tableRight = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExprList[tableRight].tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableLeft].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableLeft];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableLeft];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
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
            } else {
                if (!veloxExpr.hashJoinVec.empty()) {
                    for (auto itJoin = veloxExpr.hashJoinVec.begin(); itJoin != veloxExpr.hashJoinVec.end(); ++itJoin) {
                        auto const &hashJoinPair = *itJoin;
                        auto idxLeft = fileColumnNames.find(hashJoinPair.leftKey);
                        auto idxRight = fileColumnNames.find(hashJoinPair.rightKey);
                        if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                            int tableIdx = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.leftKey},
                                    {hashJoinPair.rightKey},
                                    build,
                                    "",
                                    outputLayout);
                        } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                            int tableIdx = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.rightKey},
                                    {hashJoinPair.leftKey},
                                    build,
                                    "",
                                    outputLayout);
                        } else {  // both left and right key are not in the current table
                            int tableLeft = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.leftKey) != columnAliaseList[j].end()) {
                                    tableLeft = j;
                                    break;
                                }
                            }
                            int tableRight = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(hashJoinPair.rightKey) != columnAliaseList[j].end()) {
                                    tableRight = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExprList[tableRight].tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableLeft].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableLeft];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableLeft];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
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
                if (!veloxExpr.hashJoinListVec.empty()) {
                    for (auto itJoin = veloxExpr.hashJoinListVec.begin();
                         itJoin != veloxExpr.hashJoinListVec.end(); ++itJoin) {
                        auto const &hashJoinPair = *itJoin;
                        auto leftKey = hashJoinPair.leftKeys[0];
                        auto rightKey = hashJoinPair.rightKeys[0];
                        auto idxLeft = fileColumnNames.find(leftKey);
                        auto idxRight = fileColumnNames.find(rightKey);
                        if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                            int tableIdx = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.leftKeys},
                                    {hashJoinPair.rightKeys},
                                    build,
                                    "",
                                    outputLayout);
                        } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                            int tableIdx = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                    tableIdx = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExpr.tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableIdx].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableIdx];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableIdx];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
                                joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                            }
                            plan.hashJoin(
                                    {hashJoinPair.rightKeys},
                                    {hashJoinPair.leftKeys},
                                    build,
                                    "",
                                    outputLayout);
                        } else {  // both left and right key are not in the current table
                            int tableLeft = 0; // find left key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(leftKey) != columnAliaseList[j].end()) {
                                    tableLeft = j;
                                    break;
                                }
                            }
                            int tableRight = 0; // find right key table
                            for (int j = 0; j < columnAliaseList.size(); j++) {
                                if (columnAliaseList[j].find(rightKey) != columnAliaseList[j].end()) {
                                    tableRight = j;
                                    break;
                                }
                            }
                            auto tableName = veloxExprList[tableRight].tableName;
                            auto it = joinMapPlan.find(tableName);
                            if (it == joinMapPlan.end()) {
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                outputLayout = mergeColumnNames(outputLayout, veloxExpr.selectedColumns);
                            }
                            tableName = veloxExprList[tableLeft].tableName;
                            it = joinMapPlan.find(tableName);
                            core::PlanNodePtr build;
                            if (it == joinMapPlan.end()) {  // first time
                                joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                                build = tableMapPlan[tableLeft];
                                outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                            } else {
                                if (it->second == MAX_JOIN_WAY) {
                                    build = tableMapPlan[tableLeft];
                                } else {
                                    build = tableMapPlan[it->second];
                                }
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
            if (!veloxExpr.filter.empty()) {
                plan.filter(veloxExpr.filter);
            }
            if (!veloxExpr.orderByVec.empty()) {
                if (veloxExpr.limit > 0) {
                    plan.topN(veloxExpr.orderByVec, veloxExpr.limit, false);
                } else {
                    plan.orderBy(veloxExpr.orderByVec, false);
                }
            } else if (veloxExpr.limit > 0) {
                plan.limit(0, veloxExpr.limit, false);
            }
            auto planPtr = plan.planNode();

            outputLayout = planPtr->outputType()->names();
            veloxExpr.outColumns = outputLayout;
            tableMapPlan.push_back(std::move(planPtr));
        }
#ifdef DebugInfo
        std::cout << "VeloxPlanBuilder Finished." << std::endl;
#endif
        return tableMapPlan.back();
    }

} // namespace boss::engines::velox
