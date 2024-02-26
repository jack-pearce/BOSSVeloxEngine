#include "BOSSQueryBuilder.h"
#include "CommonUtilities.h"

namespace boss::engines::velox {

    void QueryBuilder::add_tmpFieldFilter(std::string data, ColumnType type) {
        AtomicExpr element;
        element.data = std::move(data);
        element.type = type;
        tmpFieldFilter.element.emplace_back(element);
    }

    // avoid repeated selectedColumns
    void QueryBuilder::add_selectedColumns(const std::string &colName) {
        if (std::find(curVeloxExpr.selectedColumns.begin(),
                      curVeloxExpr.selectedColumns.end(),
                      colName) == curVeloxExpr.selectedColumns.end()) {
            curVeloxExpr.selectedColumns.push_back(colName);
        }
    }

    void QueryBuilder::postTransFilter_Join(const std::string &headName) {
        if (!tmpFieldFilter.opName.empty()) {
            if (headName == "Greater") {
                mergeGreaterFilter(tmpFieldFilter);
            } else if ((headName == "Equal" || headName == "StringContainsQ") &&
                       !tmpFieldFilter.element.empty()) {
                curVeloxExpr.tmpFieldFiltersVec.push_back(tmpFieldFilter);
            }
        }
        if (headName == "List") {
            if (!tmpJoinPairList.leftFlag) {
                tmpJoinPairList.leftFlag = true;
            } else {
                curVeloxExpr.hashJoinListVec.push_back(tmpJoinPairList);
                if (!curVeloxExpr.hashJoinVec.empty()) {
                    curVeloxExpr.delayJoinList = true;
                }
                tmpJoinPairList.clear();
            }
        }
    }

    void QueryBuilder::postTransProj_PartialAggr(std::vector<std::string> &projectionList,
                                                 std::vector<std::string> lastProjectionsVec,
                                                 const std::string &projectionName) {
        // fill the new name for aggregation
        if (!curVeloxExpr.aggregatesVec.empty()) {
            auto &aggregation = curVeloxExpr.aggregatesVec.back();
            if (aggregation.newName == "") {
                aggregation.newName = projectionName;
                curVeloxExpr.orderBy = true;
            }
            curVeloxExpr.projectionsVec = std::move(lastProjectionsVec);
        } else {
            std::string projection;
            if (projectionList[0] != projectionName) {
                projection = fmt::format("{} AS {}", projectionList[0],
                                         projectionName);
            } else {
                auto it = projNameMap.find(projectionName);
                if (it != projNameMap.end() && it->second != it->first) {
                    auto tmp = fmt::format("{} AS {}", it->second, it->first);
                    projection = tmp; // e.g. cal AS cal
                } else {
                    projection = projectionList[0]; // e.g. col0 AS col0
                }
            }
            curVeloxExpr.projectionsVec.push_back(projection);
            // avoid repeated projectionMap
            auto it = projNameMap.find(projectionName);
            if (it == projNameMap.end()) {
                projNameMap.emplace(projectionName, projectionList[0]);
            }
        }
    }

    void QueryBuilder::postTransSum(const std::string &oldName) {
        auto aName = fmt::format("a{}", aggrNameMap.size());
        aggrPair const aggregation("sum", oldName, aName);
        curVeloxExpr.aggregatesVec.push_back(aggregation);
        // new implicit name for maybe later orderBy
        aggrNameMap.emplace(aggregation.oldName, aName);
        curVeloxExpr.orderBy = true;
        add_selectedColumns(aggregation.oldName);
    }

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
        auto handleInput = [&input, this](int idx) {
            auto colName = input.element[idx].data;
            for (auto &oldInput: curVeloxExpr.tmpFieldFiltersVec) {
                if (oldInput.element[idx].type == cValue &&
                    oldInput.element[1 - idx].type == cName &&
                    oldInput.element[1 - idx].data == colName) {
                    oldInput.opName = "Between";  // replace the old Greater with Between
                    oldInput.element.push_back(input.element[1 - idx]);
                    return;
                }
            }
            curVeloxExpr.tmpFieldFiltersVec.push_back(input);
        };
        if (input.element[0].type == cName) {
            handleInput(0);
        } else if (input.element[1].type == cName) {
            handleInput(1);
        }
    }

    void QueryBuilder::formatVeloxFilter_Join() {
        curVeloxExpr.fieldFiltersVec.clear();
        curVeloxExpr.hashJoinVec.clear();
        std::unordered_map<std::string, TypePtr> fileColumnNames{};
        if (curVeloxExpr.tableName != "tmp") {
            fileColumnNames = curVeloxExpr.fileColumnNamesMap;
        }

        auto formatGreater = [this, &fileColumnNames](auto &&filter, auto idx, auto op) {
            auto tmp = fmt::format("{} {} {}", filter.element[idx].data, op,
                                   filter.element[1 - idx].data); // the column name should be on the left side.
            if (curVeloxExpr.tableName != "tmp" &&
                fileColumnNames.find(filter.element[idx].data) == fileColumnNames.end()) {
                curVeloxExpr.filter = std::move(tmp);  // not belong to any field
            } else {
                curVeloxExpr.fieldFiltersVec.emplace_back(tmp);
            }
        };

        for (auto &filter: curVeloxExpr.tmpFieldFiltersVec) {
            if (filter.opName == "Greater") {
                if (filter.element[0].type == cName && filter.element[1].type == cValue) {
                    formatGreater(filter, 0, ">");
                } else if (filter.element[1].type == cName && filter.element[0].type == cValue) {
                    formatGreater(filter, 1, "<");
                }
            } else if (filter.opName == "Between") {
                std::string tmp;
                if (filter.element[0].type == cName) {
                    tmp = fmt::format("{} between {} and {}", filter.element[0].data, filter.element[1].data,
                                      filter.element[2].data);
                } else if (filter.element[1].type == cName) {
                    tmp = fmt::format("{} between {} and {}", filter.element[1].data, filter.element[2].data,
                                      filter.element[0].data);
                }
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

    // Old format: "Table"_("Column"_("Id"_, "List"_(1, 2, 3), "Column"_("Value"_
    // "List"_(0.1, 10.0, 5.2))) New format: "Table"_("Id"_("List"_(1, 2, 3)),
    // "Value"_("List"_(0.1, 10.0, 5.2))))
    std::unordered_map<std::string, TypePtr>
    getColumns(ComplexExpression&& expression, std::vector<BufferPtr> const& indicesVec,
               size_t indicesColumnEndIndex, std::vector<RowVectorPtr>& rowDataVec,
               RowTypePtr& tableSchema, std::vector<size_t>& spanRowCountVec,
               memory::MemoryPool* pool) {
      ExpressionArguments columns = std::move(expression).getArguments();
      std::vector<std::string> colNameVec;
      std::vector<std::shared_ptr<const Type>> colTypeVec;
      std::vector<std::vector<VectorPtr>> colDataListVec;
      std::unordered_map<std::string, TypePtr> fileColumnNamesMap(columns.size());

      std::for_each(
          std::make_move_iterator(columns.begin()), std::make_move_iterator(columns.end()),
          [&colNameVec, &colTypeVec, &colDataListVec, pool, &fileColumnNamesMap,
           &indicesVec, &indicesColumnEndIndex](auto&& columnExpr) {
            auto column = get<ComplexExpression>(std::forward<decltype(columnExpr)>(columnExpr));
            auto [head, unused_, dynamics, spans] = std::move(column).decompose();

#ifdef USE_NEW_TABLE_FORMAT
            auto columnName = head.getName();
            auto dynamic = get<ComplexExpression>(std::move(dynamics.at(0)));
#else
            auto columnName = get<Symbol>(std::move(dynamics.at(0))).getName();
            auto dynamic = get<ComplexExpression>(std::move(dynamics.at(1)));
#endif
            std::transform(columnName.begin(), columnName.end(), columnName.begin(), ::tolower);

            auto list = transformDynamicsToSpans(std::move(dynamic));
            auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();
            if(listSpans.empty()) {
              return;
            }

            int numSpan = 0;
            std::vector<VectorPtr> colDataVec;
            for(auto& subSpan : listSpans) {
              BufferPtr indices = nullptr;
              if(!indicesVec.empty() && colNameVec.size() < indicesColumnEndIndex) {
                indices = indicesVec[numSpan++];
              }
              VectorPtr subColData = std::visit(
                  [pool, &indices, &columnName]<typename T>(boss::Span<T>&& typedSpan) -> VectorPtr {
                    if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
                                 std::is_same_v<T, double_t> || std::is_same_v<T, int32_t const> ||
                                 std::is_same_v<T, int64_t const> || std::is_same_v<T, double_t const>) {
                      return spanToVelox<T>(std::move(typedSpan), pool, indices);
                    } else {
                      throw std::runtime_error("unsupported column type: '" + columnName +
                          "' with type: " + std::string(typeid(decltype(typedSpan)).name()));
                    }
                  },
                  std::move(subSpan));
              colDataVec.push_back(std::move(subColData));
            }
            auto columnType = colDataVec[0]->type();
            colNameVec.emplace_back(columnName);
            colTypeVec.emplace_back(columnType);
            fileColumnNamesMap.insert(std::make_pair(columnName, columnType));
            colDataListVec.push_back(std::move(colDataVec));
          });

      // TODO this doesn't seem to handle dictionary encoded strings from int32 support branch of
      // Arrow
      auto listSize = colDataListVec[0].size();
      for(auto i = 0; i < listSize; i++) {
        spanRowCountVec.push_back(colDataListVec[0][i]->size());
        std::vector<VectorPtr> rowData;
        for(auto j = 0; j < columns.size(); j++) {
          assert(colDataListVec[j].size() == listSize);
          rowData.push_back(std::move(colDataListVec[j][i]));
        }
        auto rowVector = makeRowVectorNoCopy(colNameVec, rowData, pool);
        rowDataVec.push_back(std::move(rowVector));
      }

      tableSchema =
          TypeFactory<TypeKind::ROW>::create(std::move(colNameVec), std::move(colTypeVec));
      return fileColumnNamesMap;
    }

    void QueryBuilder::getTableMeta(ComplexExpression&& expression) {
      // temp workaround: we lost the indices when curVeloxExpr is cleared
      // (because they are set before the table is parsed)
      auto indicesVec = curVeloxExpr.indicesVec;
      auto indicesColumnEndIndex = curVeloxExpr.indicesColumnEndIndex;

      if(!curVeloxExpr.tableName.empty()) {
        veloxExprList.push_back(curVeloxExpr);
        curVeloxExpr.clear();
      }
      curVeloxExpr.tableName = fmt::format("Table{}", tableCnt++); // indicate table names
      curVeloxExpr.fileColumnNamesMap = getColumns(
          std::move(expression), indicesVec, indicesColumnEndIndex,
          curVeloxExpr.rowDataVec, curVeloxExpr.tableSchema, curVeloxExpr.spanRowCountVec, &pool_);
      // std::cout << curVeloxExpr.tableName << ":" << std::endl;
      /*auto inputSize =
          std::accumulate(curVeloxExpr.rowDataVec.begin(), curVeloxExpr.rowDataVec.end(), 0,
                                       [](size_t total, auto const& vec) {
            std::cout << "  span size: " << vec->size() << std::endl;
            return total + vec->size();
          });
      std::cout << " input size: " << inputSize << std::endl;*/
    }

    void QueryBuilder::getFileColumnNamesMap() {
        veloxExprList.push_back(curVeloxExpr); //push back the last expression to the vector
        for (auto &expr: veloxExprList) {
            columnAliaseList.emplace_back(expr.fileColumnNamesMap);
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

    core::PlanNodePtr QueryBuilder::getVeloxPlanBuilder(std::vector<core::PlanNodeId> &scanIds) {
        const long MAX_JOIN_WAY = 0xff;
        auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
#ifdef SUPPORT_RADIX_JOINS
        std::vector<std::vector<PlanBuilder>> tableMapPlan;
#else
        std::vector<PlanBuilder> tableMapPlan;
#endif // SUPPORT_RADIX_JOINS
        std::unordered_map<std::string, long> joinMapPlan;
        std::vector<std::string> outputLayout;

        for (auto itExpr = veloxExprList.begin(); itExpr != veloxExprList.end(); ++itExpr) {
            auto &veloxExpr = *itExpr;
            const auto &fileColumnNames = veloxExpr.fileColumnNamesMap;

            auto assignColumns = [](std::vector<std::string> names) {
              std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
                  assignmentsMap;
              assignmentsMap.reserve(names.size());
              for(auto& name : names) {
                assignmentsMap.emplace(name, std::make_shared<BossColumnHandle>(name));
              }
              return assignmentsMap;
            };

            // nothing happened for a table, projection for all columns
            if(veloxExpr.selectedColumns.empty()) {
              assert(veloxExpr.projectionsVec.empty());
              std::for_each(fileColumnNames.begin(), fileColumnNames.end(), [&veloxExpr](auto&& p) {
                veloxExpr.selectedColumns.push_back(p.first);
              });
              veloxExpr.projectionsVec = veloxExpr.selectedColumns;
            }

            auto assignmentsMap = assignColumns(veloxExpr.tableSchema->names());
            auto createScanProjectFilterPlan = [&assignmentsMap, &veloxExpr, &scanIds,
                                                &planNodeIdGenerator](int offset = 0,
                                                                      int partitionSize = -1) {
              core::PlanNodeId scanId;
              auto plan = PlanBuilder(planNodeIdGenerator)
                              .startTableScan()
                              .outputType(veloxExpr.tableSchema)
                              .tableHandle(std::make_shared<BossTableHandle>(
                                  kBossConnectorId, veloxExpr.tableName, veloxExpr.tableSchema,
                                  veloxExpr.rowDataVec, veloxExpr.spanRowCountVec))
                              .assignments(assignmentsMap)
                              .endTableScan()
                              .capturePlanNodeId(scanId);
              //std::cout << "add scanId: " << scanId << std::endl;
              scanIds.emplace_back(scanId);
              // limit
              if(partitionSize >= 0) {
                plan.limit(offset, partitionSize, true);
              }
              // project involved columns only
              plan.project(veloxExpr.selectedColumns);
              // filter
              if(!veloxExpr.fieldFiltersVec.empty()) {
                auto filtersCnt = 0;
                while(filtersCnt < veloxExpr.fieldFiltersVec.size()) {
                  plan.filter(veloxExpr.fieldFiltersVec[filtersCnt++]);
                }
              }
              return plan;
            };

            std::optional<PlanBuilder> plan;

            auto addHashJoinToPlan = [&plan, &outputLayout, &planNodeIdGenerator, &veloxExpr,
                                      &createScanProjectFilterPlan](auto& hashKeys0,
                                                                    auto& hashKeys1,
#ifdef SUPPORT_RADIX_JOINS
                                                                    auto& builds) {
              auto buildIt = builds.begin();
              if(veloxExpr.radixPartitions.empty()) {
                // classic hash join
                if(!plan) {
                  plan = createScanProjectFilterPlan();
                }
                plan->hashJoin(hashKeys0, hashKeys1, buildIt->planNode(), "", outputLayout);
              } else {
                // radix hash join
                int64_t offset = 0;
                std::vector<core::PlanNodePtr> partialJoinNodes;
                for(auto& partitionSize : veloxExpr.radixPartitions) {
                  auto partialJoin = createScanProjectFilterPlan(offset, partitionSize);
                  auto radixKeys0 = hashKeys0;
                  // std::transform(radixKeys0.begin(), radixKeys0.end(), radixKeys0.begin(),
                  //                [](auto& str) { return "radix_" + str; });
                  auto radixKeys1 = hashKeys1;
                  // std::transform(radixKeys1.begin(), radixKeys1.end(), radixKeys1.begin(),
                  //                [](auto& str) { return "radix_" + str; });
                  partialJoin.hashJoin(radixKeys0, radixKeys1, (buildIt++)->planNode(), "",
                                       outputLayout);
                  partialJoinNodes.emplace_back(partialJoin.planNode());
                  offset += partitionSize;
                }
                plan = PlanBuilder(planNodeIdGenerator).localPartition({}, partialJoinNodes);
              }
#else
                                                                    auto& build) {
              if(!plan) {
                plan = createScanProjectFilterPlan();
              }
              plan->hashJoin(hashKeys0, hashKeys1, build.planNode(), "", outputLayout);
#endif // SUPPORT_RADIX_JOINS
            };

            auto join_find_key1 = [this, &veloxExpr, &joinMapPlan, &MAX_JOIN_WAY, &outputLayout,
                                   &tableMapPlan, &itExpr, &plan, &addHashJoinToPlan](
                    auto key1, const std::vector<std::string> &hashKeys0, const std::vector<std::string> &hashKeys1) {
                int tableIdx = 0;
                for (int j = 0; j < columnAliaseList.size(); j++) {
                    if (columnAliaseList[j].find(key1) != columnAliaseList[j].end()) {
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
                int64_t buildPlanIdx = 0;
                if (it == joinMapPlan.end()) {  // first time
                    joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                    buildPlanIdx = tableIdx;
                    outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableIdx].outColumns);
                } else {
                    if (it->second == MAX_JOIN_WAY) {
                        buildPlanIdx = tableIdx;
                    } else {
                        buildPlanIdx = it->second;
                    }
                    joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                }
                addHashJoinToPlan(hashKeys0, hashKeys1, tableMapPlan[buildPlanIdx]);
            };

            auto join_find_key2 =
                [this, &veloxExpr, &joinMapPlan, &MAX_JOIN_WAY, &outputLayout, &tableMapPlan,
                 &itExpr, &plan, &addHashJoinToPlan](
                    auto leftKey, auto rightKey, const std::vector<std::string> &hashKeys0,
                    const std::vector<std::string> &hashKeys1) {
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
                int64_t buildPlanIdx = 0;
                if (it == joinMapPlan.end()) {  // first time
                    joinMapPlan.emplace(tableName, MAX_JOIN_WAY);
                  buildPlanIdx = tableLeft;
                    outputLayout = mergeColumnNames(outputLayout, veloxExprList[tableLeft].selectedColumns);
                } else {
                    if (it->second == MAX_JOIN_WAY) {
                    buildPlanIdx = tableLeft;
                    } else {
                      buildPlanIdx = it->second;
                    }
                    joinMapPlan[tableName] = itExpr - veloxExprList.begin();
                }
                addHashJoinToPlan(hashKeys0, hashKeys1, tableMapPlan[buildPlanIdx]);
            };

            auto join_handle = [&]() {
                for (const auto &hashJoinPair: veloxExpr.hashJoinVec) {
                    auto idxLeft = fileColumnNames.find(hashJoinPair.leftKey);
                    auto idxRight = fileColumnNames.find(hashJoinPair.rightKey);
                    if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                        // find right key table
                        join_find_key1(hashJoinPair.rightKey, {hashJoinPair.leftKey}, {hashJoinPair.rightKey});
                    } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                        // find left key table
                        join_find_key1(hashJoinPair.leftKey, {hashJoinPair.rightKey}, {hashJoinPair.leftKey});
                    } else {
                        // both left and right key are not in the current table
                        join_find_key2(hashJoinPair.leftKey, hashJoinPair.rightKey, {hashJoinPair.rightKey},
                                       {hashJoinPair.leftKey});
                    }
                }
            };

            auto joinList_handle = [&]() {
                for (const auto &hashJoinPair: veloxExpr.hashJoinListVec) {
                    auto leftKey = hashJoinPair.leftKeys[0];
                    auto rightKey = hashJoinPair.rightKeys[0];
                    auto idxLeft = fileColumnNames.find(leftKey);
                    auto idxRight = fileColumnNames.find(rightKey);
                    if (idxLeft != fileColumnNames.end() && idxRight == fileColumnNames.end()) {
                        // find right key table
                        join_find_key1(rightKey, hashJoinPair.leftKeys, hashJoinPair.rightKeys);

                    } else if (idxLeft == fileColumnNames.end() && idxRight != fileColumnNames.end()) {
                        // find left key table
                        join_find_key1(leftKey, hashJoinPair.rightKeys, hashJoinPair.leftKeys);
                    } else {
                        // both left and right key are not in the current table
                        join_find_key2(leftKey, rightKey, hashJoinPair.rightKeys, hashJoinPair.leftKeys);
                    }
                }
            };

            // list join first
            if (!veloxExpr.delayJoinList) {
                if (!veloxExpr.hashJoinListVec.empty()) {
                    joinList_handle();
                }
                if (!veloxExpr.hashJoinVec.empty()) {
                    join_handle();
                }
            } else {
                if (!veloxExpr.hashJoinVec.empty()) {
                    join_handle();
                }
                if (!veloxExpr.hashJoinListVec.empty()) {
                    joinList_handle();
                }
            }

            auto finalizePlan = [&veloxExpr, &itExpr, this](auto& plan) {
              if(!veloxExpr.projectionsVec.empty()) {
                plan.project(veloxExpr.projectionsVec);
              }
              if(!veloxExpr.groupingKeysVec.empty() || !veloxExpr.aggregatesVec.empty()) {
                std::vector<std::string> aggregatesVec;
                veloxExpr.selectedColumns = veloxExpr.groupingKeysVec;
                for(auto itAggr = veloxExpr.aggregatesVec.begin();
                    itAggr != veloxExpr.aggregatesVec.end(); ++itAggr) {
                  auto aggregation = *itAggr;
                  auto tmp = fmt::format("{}({}) as {}", aggregation.op, aggregation.oldName,
                                         aggregation.newName);
                  aggregatesVec.emplace_back(tmp);
                  veloxExpr.selectedColumns.emplace_back(aggregation.newName);
                }
                plan.partialAggregation(veloxExpr.groupingKeysVec, aggregatesVec);
              }
              if((itExpr == veloxExprList.end() - 1) &&
                 (!veloxExpr.groupingKeysVec.empty() || !veloxExpr.aggregatesVec.empty())) {
                plan.localPartition({});
                plan.finalAggregation();
              }
              if(!veloxExpr.filter.empty()) {
                plan.filter(veloxExpr.filter);
              }
              if(!veloxExpr.orderByVec.empty()) {
                if(veloxExpr.limit > 0) {
                  plan.topN(veloxExpr.orderByVec, veloxExpr.limit, false);
                } else {
                  plan.orderBy(veloxExpr.orderByVec, false);
                }
              } else if(veloxExpr.limit > 0) {
                plan.limit(0, veloxExpr.limit, false);
              }
            };

#ifdef SUPPORT_RADIX_JOINS
            if(!plan) {
              if(veloxExpr.radixPartitions.empty()) {
                plan = createScanProjectFilterPlan();
                finalizePlan(*plan);
                tableMapPlan.push_back({std::move(*plan)});
              } else {
                tableMapPlan.push_back({});
                auto& partialPlans = tableMapPlan.back();
                int64_t offset = 0;
                for(auto& partitionSize : veloxExpr.radixPartitions) {
                  auto partialPlan = createScanProjectFilterPlan(offset, partitionSize);
                  finalizePlan(partialPlan);
                  partialPlans.push_back(std::move(partialPlan));
                  offset += partitionSize;
                }
              }
            } else {
              finalizePlan(*plan);
              tableMapPlan.push_back({std::move(*plan)});
            }
            outputLayout = tableMapPlan.back()[0].planNode()->outputType()->names();
#else
            if(!plan) {
              plan = createScanProjectFilterPlan();
            }
            finalizePlan(*plan);
            tableMapPlan.push_back(std::move(*plan));
            outputLayout = tableMapPlan.back().planNode()->outputType()->names();
#endif // SUPPORT_RADIX_JOINS
            veloxExpr.outColumns = outputLayout;
        }
#ifdef DebugInfo
        std::cout << "VeloxPlanBuilder Finished." << std::endl;
#endif
#ifdef SUPPORT_RADIX_JOINS
        return tableMapPlan.back()[0].planNode();
#else
        return tableMapPlan.back().planNode();
#endif // SUPPORT_RADIX_JOINS
    }

} // namespace boss::engines::velox
