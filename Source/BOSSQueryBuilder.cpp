#include "BOSSQueryBuilder.h"

#include "velox/common/base/Fs.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::exec::test {

namespace {
int64_t toDate(std::string_view stringDate) {
  Date date;
  parseTo(stringDate, date);
  return date.days();
}

/// DWRF does not support Date type and Varchar is used.
/// Return the Date filter expression as per data format.
std::string formatDateFilter(
    const std::string& stringDate,
    const RowTypePtr& rowType,
    const std::string& lowerBound,
    const std::string& upperBound) {
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
    const std::vector<std::string>& firstColumnVector,
    const std::vector<std::string>& secondColumnVector) {
  std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
  mergedColumnVector.insert(
      mergedColumnVector.end(),
      secondColumnVector.begin(),
      secondColumnVector.end());
  return mergedColumnVector;
};
} // namespace

void BossQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        dwio::common::ReaderOptions readerOptions;
        readerOptions.setFileFormat(format_);
        std::unique_ptr<dwio::common::Reader> reader =
            dwio::common::getReaderFactory(readerOptions.getFileFormat())
                ->createReader(
                    std::make_unique<dwio::common::FileInputStream>(
                        dirEntry.path()),
                    readerOptions);
        const auto fileType = reader->rowType();
        const auto fileColumnNames = fileType->names();
        // There can be extra columns in the file towards the end.
        VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
        std::unordered_map<std::string, std::string> fileColumnNamesMap(
            columns.size());
        std::transform(
            columns.begin(),
            columns.end(),
            fileColumnNames.begin(),
            std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
            [](std::string a, std::string b) { return std::make_pair(a, b); });
        auto columnNames = columns;
        auto types = fileType->children();
        types.resize(columnNames.size());
        tableMetadata_[tableName].type =
            std::make_shared<RowType>(std::move(columnNames), std::move(types));
        tableMetadata_[tableName].fileColumnNames =
            std::move(fileColumnNamesMap);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
  }
}

const std::vector<std::string>& BossQueryBuilder::getTableNames() {
  return kTableNames_;
}

//created by Meta, reserved for debugging
BossPlan BossQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getQ1Plan();
    case 3:
      return getQ3Plan();
    case 5:
      return getQ5Plan();
    case 6:
      return getQ6Plan();
    case 7:
      return getQ7Plan();
    case 8:
      return getQ8Plan();
    case 9:
      return getQ9Plan();
    case 10:
      return getQ10Plan();
    case 12:
      return getQ12Plan();
    case 13:
      return getQ13Plan();
    case 14:
      return getQ14Plan();
    case 15:
      return getQ15Plan();
    case 16:
      return getQ16Plan();
    case 18:
      return getQ18Plan();
    case 19:
      return getQ19Plan();
    case 22:
      return getQ22Plan();
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

BossPlan BossQueryBuilder::getQ1Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_returnflag",
      "l_linestatus",
      "l_quantity",
      "l_extendedprice",
      "l_discount",
      "l_tax",
      "l_shipdate"};

  const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  // shipdate <= '1998-09-02'
  const auto shipDate = "l_shipdate";
  auto filter = formatDateFilter(shipDate, selectedRowType, "", "'1998-09-03'");

  core::PlanNodeId lineitemPlanNodeId;

  auto plan =
      PlanBuilder()
          .tableScan(kLineitem, selectedRowType, fileColumnNames, {filter})
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(
              {"l_returnflag",
               "l_linestatus",
               "l_quantity",
               "l_extendedprice",
               "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
               "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
               "l_discount"})
          .partialAggregation(
              {"l_returnflag", "l_linestatus"},
              {"sum(l_quantity)",
               "sum(l_extendedprice)",
               "sum(l_sum_disc_price)",
               "sum(l_sum_charge)",
               "avg(l_quantity)",
               "avg(l_extendedprice)",
               "avg(l_discount)",
               "count(0)"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_returnflag", "l_linestatus"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ3Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_shippriority", "o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_mktsegment"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  const auto orderDate = "o_orderdate";
  const auto shipDate = "l_shipdate";
  auto orderDateFilter =
      formatDateFilter(orderDate, ordersSelectedRowType, "", "'1995-03-15'");
  auto shipDateFilter =
      formatDateFilter(shipDate, lineitemSelectedRowType, "'1995-03-15'", "");
  auto customerFilter = "c_mktsegment = 'BUILDING'";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;
  core::PlanNodeId ordersPlanNodeId;
  core::PlanNodeId customerPlanNodeId;

  auto customers = PlanBuilder(planNodeIdGenerator)
                       .tableScan(
                           kCustomer,
                           customerSelectedRowType,
                           customerFileColumns,
                           {customerFilter})
                       .capturePlanNodeId(customerPlanNodeId)
                       .planNode();

  auto custkeyJoinNode =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {orderDateFilter})
          .capturePlanNodeId(ordersPlanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers,
              "",
              {"o_orderdate", "o_shippriority", "o_orderkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
               "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              custkeyJoinNode,
              "",
              {"l_orderkey", "o_orderdate", "o_shippriority", "part_revenue"})
          .partialAggregation(
              {"l_orderkey", "o_orderdate", "o_shippriority"},
              {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .project({"l_orderkey", "revenue", "o_orderdate", "o_shippriority"})
          .orderBy({"revenue DESC", "o_orderdate"}, false)
          .limit(0, 10, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersPlanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerPlanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ5Plan() const {
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_custkey", "o_orderkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> nationColumns = {
      "n_nationkey", "n_name", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_regionkey", "r_name"};

  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  const auto& regionFileColumns = getFileColumnNames(kRegion);

  std::string regionNameFilter = "r_name = 'ASIA'";
  const auto orderDate = "o_orderdate";
  std::string orderDateFilter = formatDateFilter(
      orderDate, ordersSelectedRowType, "'1994-01-01'", "'1994-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto region = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {regionNameFilter})
                    .capturePlanNodeId(regionScanNodeId)
                    .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto customer =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              region,
              "",
              {"n_nationkey", "n_name"})
          .planNode();

  auto supplierJoinNationRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationJoinRegion,
              "",
              {"s_suppkey", "n_name", "s_nationkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
               "l_orderkey",
               "l_suppkey"})
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNationRegion,
              "",
              {"n_name", "part_revenue", "s_nationkey", "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              orders,
              "",
              {"n_name", "part_revenue", "s_nationkey", "o_custkey"})
          .hashJoin(
              {"s_nationkey", "o_custkey"},
              {"c_nationkey", "c_custkey"},
              customer,
              "",
              {"n_name", "part_revenue"})
          .partialAggregation({"n_name"}, {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"revenue DESC"}, false)
          .project({"n_name", "revenue"})
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ6Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};

  const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  const auto shipDate = "l_shipdate";
  auto shipDateFilter = formatDateFilter(
      shipDate, selectedRowType, "'1994-01-01'", "'1994-12-31'");

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder()
                  .tableScan(
                      kLineitem,
                      selectedRowType,
                      fileColumnNames,
                      {shipDateFilter,
                       "l_discount between 0.05 and 0.07",
                       "l_quantity < 24.0"})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"l_extendedprice * l_discount"})
                  .partialAggregation({}, {"sum(p0)"})
                  .localPartition({})
                  .finalAggregation()
                  .planNode();
  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ7Plan() const {
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> ordersColumns = {"o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  const std::string nationFilter = "n_name IN ('FRANCE', 'GERMANY')";
  auto shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId suppNationScanNodeId;
  core::PlanNodeId custNationScanNodeId;

  auto custNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kNation, nationSelectedRowType, nationFileColumns, {nationFilter})
          .capturePlanNodeId(custNationScanNodeId)
          .planNode();

  auto customerJoinNation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              custNation,
              "",
              {"n_name", "c_custkey"})
          .project({"n_name as cust_nation", "c_custkey"})
          .planNode();

  auto ordersJoinCustomer =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customerJoinNation,
              "",
              {"cust_nation", "o_orderkey"})
          .planNode();

  auto suppNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kNation, nationSelectedRowType, nationFileColumns, {nationFilter})
          .capturePlanNodeId(suppNationScanNodeId)
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              suppNation,
              "",
              {"n_name", "s_suppkey"})
          .project({"n_name as supp_nation", "s_suppkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"supp_nation",
               "l_extendedprice",
               "l_discount",
               "l_shipdate",
               "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomer,
              "(((cust_nation = 'FRANCE') AND (supp_nation = 'GERMANY')) OR "
              "((cust_nation = 'GERMANY') AND (supp_nation = 'FRANCE')))",
              {"supp_nation",
               "cust_nation",
               "l_extendedprice",
               "l_discount",
               "l_shipdate"})
          .project(
              {"cust_nation",
               "supp_nation",
               "l_extendedprice * (1.0 - l_discount) as part_revenue",
               "year(l_shipdate) as l_year"})
          .partialAggregation(
              {"supp_nation", "cust_nation", "l_year"},
              {"sum(part_revenue) as revenue"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"supp_nation", "cust_nation", "l_year"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[suppNationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[custNationScanNodeId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ8Plan() const {
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_orderkey", "o_custkey"};
  std::vector<std::string> customerColumns = {"c_nationkey", "c_custkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_regionkey"};
  std::vector<std::string> nationColumnsWithName = {
      "n_name", "n_nationkey", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_name", "r_regionkey"};

  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  const auto nationSelectedRowTypeWithName =
      getRowType(kNation, nationColumnsWithName);
  const auto& nationFileColumnsWithName = getFileColumnNames(kNation);
  const auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  const auto& regionFileColumns = getFileColumnNames(kRegion);

  const auto orderDateFilter = formatDateFilter(
      "o_orderdate", ordersSelectedRowType, "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId nationScanNodeIdWithName;
  core::PlanNodeId regionScanNodeId;

  auto nationWithName =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kNation, nationSelectedRowTypeWithName, nationFileColumnsWithName)
          .capturePlanNodeId(nationScanNodeIdWithName)
          .planNode();

  auto region = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {"r_name = 'AMERICA'"})
                    .capturePlanNodeId(regionScanNodeId)
                    .planNode();

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {"p_type = 'ECONOMY ANODIZED STEEL'"})
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin(
              {"n_regionkey"}, {"r_regionkey"}, region, "", {"n_nationkey"})
          .planNode();

  auto customerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              nationJoinRegion,
              "",
              {"c_custkey"})
          .planNode();

  auto ordersJoinCustomerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {orderDateFilter})
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customerJoinNationJoinRegion,
              "",
              {"o_orderkey", "o_orderdate"})
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationWithName,
              "",
              {"s_suppkey", "n_name"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomerJoinNationJoinRegion,
              "",
              {"l_partkey",
               "l_suppkey",
               "o_orderdate",
               "l_extendedprice",
               "l_discount"})
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"n_name",
               "o_orderdate",
               "l_partkey",
               "l_extendedprice",
               "l_discount"})
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              part,
              "",
              {"n_name", "o_orderdate", "l_extendedprice", "l_discount"})
          .project(
              {"l_extendedprice * (1.0 - l_discount) as volume",
               "n_name",
               "o_orderdate"})
          .project(
              {"volume",
               "(CASE WHEN n_name = 'BRAZIL' THEN volume ELSE 0.0 END) as brazil_volume",
               "year(o_orderdate) AS o_year"})
          .partialAggregation(
              {"o_year"},
              {"sum(brazil_volume) as volume_brazil",
               "sum(volume) as volume_all"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"o_year"}, false)
          .project({"o_year", "(volume_brazil / volume_all) as mkt_share"})
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[nationScanNodeIdWithName] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ9Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_suppkey",
      "l_partkey",
      "l_discount",
      "l_extendedprice",
      "l_orderkey",
      "l_quantity"};
  std::vector<std::string> partColumns = {"p_name", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> partsuppColumns = {
      "ps_partkey", "ps_suppkey", "ps_supplycost"};
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderdate"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partsuppScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId nationScanNodeId;

  const std::vector<std::string> lineitemCommonColumns = {
      "l_extendedprice", "l_discount", "l_quantity"};

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {},
                      "p_name like '%green%'")
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto supplier =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .planNode();

  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .planNode();

  auto lineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin({"l_partkey"}, {"p_partkey"}, part, "", lineitemColumns)
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              mergeColumnNames(lineitemColumns, {"s_nationkey"}))
          .planNode();

  auto partsuppJoinLineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanNodeId)
          .hashJoin(
              {"ps_partkey", "ps_suppkey"},
              {"l_partkey", "l_suppkey"},
              lineitemJoinPartJoinSupplier,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"l_orderkey", "s_nationkey", "ps_supplycost"}))
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              partsuppJoinLineitemJoinPartJoinSupplier,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"s_nationkey", "ps_supplycost", "o_orderdate"}))
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"ps_supplycost", "o_orderdate", "n_name"}))
          .project(
              {"n_name AS nation",
               "year(o_orderdate) AS o_year",
               "l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity AS amount"})
          .partialAggregation(
              {"nation", "o_year"}, {"sum(amount) AS sum_profit"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"nation", "o_year DESC"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartsupp);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ10Plan() const {
  std::vector<std::string> customerColumns = {
      "c_nationkey",
      "c_custkey",
      "c_acctbal",
      "c_name",
      "c_address",
      "c_phone",
      "c_comment"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};
  std::vector<std::string> lineitemColumns = {
      "l_orderkey", "l_returnflag", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_orderkey", "o_custkey"};

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto lineitemReturnFlagFilter = "l_returnflag = 'R'";
  const auto orderDate = "o_orderdate";
  auto orderDateFilter = formatDateFilter(
      orderDate, ordersSelectedRowType, "'1993-10-01'", "'1993-12-31'");

  const std::vector<std::string> customerOutputColumns = {
      "c_name", "c_acctbal", "c_phone", "c_address", "c_custkey", "c_comment"};

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto partialPlan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              orders,
              "",
              mergeColumnNames(
                  customerOutputColumns, {"c_nationkey", "o_orderkey"}))
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              mergeColumnNames(customerOutputColumns, {"n_name", "o_orderkey"}))
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kLineitem,
                      lineitemSelectedRowType,
                      lineitemFileColumns,
                      {lineitemReturnFlagFilter})
                  .capturePlanNodeId(lineitemScanNodeId)
                  .project(
                      {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
                       "l_orderkey"})
                  .hashJoin(
                      {"l_orderkey"},
                      {"o_orderkey"},
                      partialPlan,
                      "",
                      mergeColumnNames(
                          customerOutputColumns, {"part_revenue", "n_name"}))
                  .partialAggregation(
                      {"c_custkey",
                       "c_name",
                       "c_acctbal",
                       "n_name",
                       "c_address",
                       "c_phone",
                       "c_comment"},
                      {"sum(part_revenue) as revenue"})
                  .localPartition({})
                  .finalAggregation()
                  .orderBy({"revenue DESC"}, false)
                  .project(
                      {"c_custkey",
                       "c_name",
                       "revenue",
                       "c_acctbal",
                       "n_name",
                       "c_address",
                       "c_phone",
                       "c_comment"})
                  .limit(0, 20, false)
                  .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ12Plan() const {
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderpriority"};
  std::vector<std::string> lineitemColumns = {
      "l_receiptdate",
      "l_orderkey",
      "l_commitdate",
      "l_shipmode",
      "l_shipdate"};

  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  const std::string receiptDateFilter = formatDateFilter(
      "l_receiptdate", lineitemSelectedRowType, "'1994-01-01'", "'1994-12-31'");
  const std::string shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "", "'1995-01-01'");
  const std::string commitDateFilter = formatDateFilter(
      "l_commitdate", lineitemSelectedRowType, "", "'1995-01-01'");

  auto lineitem = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(
                          kLineitem,
                          lineitemSelectedRowType,
                          lineitemFileColumns,
                          {receiptDateFilter,
                           "l_shipmode IN ('MAIL', 'SHIP')",
                           shipDateFilter,
                           commitDateFilter},
                          "l_commitdate < l_receiptdate")
                      .capturePlanNodeId(lineitemScanNodeId)
                      .filter("l_shipdate < l_commitdate")
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns, {})
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              lineitem,
              "",
              {"l_shipmode", "o_orderpriority"})
          .project(
              {"l_shipmode",
               "(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count_partial",
               "(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count_partial"})
          .partialAggregation(
              {"l_shipmode"},
              {"sum(high_line_count_partial) as high_line_count",
               "sum(low_line_count_partial) as low_line_count"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_shipmode"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ13Plan() const {
  std::vector<std::string> ordersColumns = {
      "o_custkey", "o_comment", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey"};

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {},
              "o_comment not like '%special%requests%'")
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers,
              "",
              {"c_custkey", "o_orderkey"},
              core::JoinType::kRight)
          .partialAggregation({"c_custkey"}, {"count(o_orderkey) as pc_count"})
          .localPartition({})
          .finalAggregation(
              {"c_custkey"}, {"count(pc_count) as c_count"}, {BIGINT()})
          .singleAggregation({"c_count"}, {"count(0) as custdist"})
          .orderBy({"custdist DESC", "c_count DESC"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ14Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey", "l_extendedprice", "l_discount", "l_shipdate"};
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);

  const std::string shipDate = "l_shipdate";
  const std::string shipDateFilter = formatDateFilter(
      shipDate, lineitemSelectedRowType, "'1995-09-01'", "'1995-09-30'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {},
              shipDateFilter)
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) as part_revenue",
               "l_shipdate",
               "l_partkey"})
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              part,
              "",
              {"part_revenue", "p_type"})
          .project(
              {"(CASE WHEN (p_type LIKE 'PROMO%') THEN part_revenue ELSE 0.0 END) as filter_revenue",
               "part_revenue"})
          .partialAggregation(
              {},
              {"sum(part_revenue) as total_revenue",
               "sum(filter_revenue) as total_promo_revenue"})
          .localPartition({})
          .finalAggregation()
          .project(
              {"100.00 * total_promo_revenue/total_revenue as promo_revenue"})
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ15Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"};
  std::vector<std::string> supplierColumns = {
      "s_suppkey", "s_name", "s_address", "s_phone"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);

  const std::string shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "'1996-01-01'", "'1996-03-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeIdSubQuery;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;

  auto maxRevenue =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_suppkey",
               "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation(
              {"l_suppkey"}, {"sum(part_revenue) as total_revenue"})
          .localPartition({})
          .finalAggregation()
          .singleAggregation({}, {"max(total_revenue) as max_revenue"})
          .planNode();

  auto supplierWithMaxRevenue =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeIdSubQuery)
          .project(
              {"l_suppkey as supplier_no",
               "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation(
              {"supplier_no"}, {"sum(part_revenue) as total_revenue"})
          .localPartition({})
          .finalAggregation()
          .hashJoin(
              {"total_revenue"},
              {"max_revenue"},
              maxRevenue,
              "",
              {"supplier_no", "total_revenue"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_suppkey"},
              {"supplier_no"},
              supplierWithMaxRevenue,
              "",
              {"s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"})
          .orderBy({"s_suppkey"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeIdSubQuery] = getTableFilePaths(kLineitem);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ16Plan() const {
  std::vector<std::string> partColumns = {
      "p_brand", "p_type", "p_size", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_comment"};
  std::vector<std::string> partsuppColumns = {"ps_partkey", "ps_suppkey"};

  const auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  const auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId partsuppScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {"p_size in (49, 14, 23, 45, 19, 3, 36, 9)"},
                      "p_type NOT LIKE 'MEDIUM POLISHED%'")
                  .capturePlanNodeId(partScanNodeId)
                  // Neq is unsupported as a tableScan subfield filter for
                  // Parquet source.
                  .filter("p_brand <> 'Brand#45'")
                  .planNode();

  auto supplier = PlanBuilder(planNodeIdGenerator)
                      .tableScan(
                          kSupplier,
                          supplierSelectedRowType,
                          supplierFileColumns,
                          {},
                          "s_comment LIKE '%Customer%Complaints%'")
                      .capturePlanNodeId(supplierScanNodeId)
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanNodeId)
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              part,
              "",
              {"ps_suppkey", "p_brand", "p_type", "p_size"})
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              {"ps_suppkey", "p_brand", "p_type", "p_size"},
              core::JoinType::kNullAwareAnti)
          // Empty aggregate is used here to get the distinct count of
          // ps_suppkey.
          // approx_distinct could be used instead for getting the count of
          // distinct ps_suppkey but since approx_distinct is non deterministic
          // and the standard error can not be set to 0, it is not used here.
          .partialAggregation({"p_brand", "p_type", "p_size", "ps_suppkey"}, {})
          .localPartition({"p_brand", "p_type", "p_size", "ps_suppkey"})
          .finalAggregation()
          .partialAggregation(
              {"p_brand", "p_type", "p_size"},
              {"count(ps_suppkey) as supplier_cnt"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"supplier_cnt DESC", "p_brand", "p_type", "p_size"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartsupp);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ18Plan() const {
  std::vector<std::string> lineitemColumns = {"l_orderkey", "l_quantity"};
  std::vector<std::string> ordersColumns = {
      "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"};
  std::vector<std::string> customerColumns = {"c_name", "c_custkey"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  auto bigOrders =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .partialAggregation(
              {"l_orderkey"}, {"sum(l_quantity) AS partial_sum"})
          .localPartition({"l_orderkey"})
          .finalAggregation(
              {"l_orderkey"}, {"sum(partial_sum) AS quantity"}, {DOUBLE()})
          .filter("quantity > 300.0")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              bigOrders,
              "",
              {"o_orderkey",
               "o_custkey",
               "o_orderdate",
               "o_totalprice",
               "l_orderkey",
               "quantity"})
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kCustomer, customerSelectedRowType, customerFileColumns)
                  .capturePlanNodeId(customerScanNodeId)
                  .planNode(),
              "",
              {"c_name",
               "c_custkey",
               "o_orderkey",
               "o_orderdate",
               "o_totalprice",
               "quantity"})
          .localPartition({})
          .orderBy({"o_totalprice DESC", "o_orderdate"}, false)
          .limit(0, 100, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ19Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey",
      "l_shipmode",
      "l_shipinstruct",
      "l_extendedprice",
      "l_discount",
      "l_quantity"};
  std::vector<std::string> partColumns = {
      "p_partkey", "p_brand", "p_container", "p_size"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  const std::string shipModeFilter = "l_shipmode IN ('AIR', 'AIR REG')";
  const std::string shipInstructFilter =
      "(l_shipinstruct = 'DELIVER IN PERSON')";
  const std::string joinFilterExpr =
      "     ((p_brand = 'Brand#12')"
      "     AND (l_quantity between 1.0 and 11.0)"
      "     AND (p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))"
      "     AND (p_size BETWEEN 1 AND 5))"
      " OR  ((p_brand ='Brand#23')"
      "     AND (p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))"
      "     AND (l_quantity between 10.0 and 20.0)"
      "     AND (p_size BETWEEN 1 AND 10))"
      " OR  ((p_brand = 'Brand#34')"
      "     AND (p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))"
      "     AND (l_quantity between 20.0 and 30.0)"
      "     AND (p_size BETWEEN 1 AND 15))";

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kLineitem,
                      lineitemSelectedRowType,
                      lineitemFileColumns,
                      {shipModeFilter, shipInstructFilter})
                  .capturePlanNodeId(lineitemScanNodeId)
                  .project(
                      {"l_extendedprice * (1.0 - l_discount) as part_revenue",
                       "l_shipmode",
                       "l_shipinstruct",
                       "l_partkey",
                       "l_quantity"})
                  .hashJoin(
                      {"l_partkey"},
                      {"p_partkey"},
                      part,
                      joinFilterExpr,
                      {"part_revenue"})
                  .partialAggregation({}, {"sum(part_revenue) as revenue"})
                  .localPartition({})
                  .finalAggregation()
                  .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

BossPlan BossQueryBuilder::getQ22Plan() const {
  std::vector<std::string> ordersColumns = {"o_custkey"};
  std::vector<std::string> customerColumns = {"c_acctbal", "c_phone"};
  std::vector<std::string> customerColumnsWithKey = {
      "c_custkey", "c_acctbal", "c_phone"};

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto customerSelectedRowTypeWithKey =
      getRowType(kCustomer, customerColumnsWithKey);
  const auto& customerFileColumnsWithKey = getFileColumnNames(kCustomer);

  const std::string phoneFilter =
      "substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId customerScanNodeIdWithKey;
  core::PlanNodeId ordersScanNodeId;

  auto orders =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .planNode();

  auto customerAvgAccountBalance =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kCustomer,
              customerSelectedRowType,
              customerFileColumns,
              {"c_acctbal > 0.0"},
              phoneFilter)
          .capturePlanNodeId(customerScanNodeId)
          .partialAggregation({}, {"avg(c_acctbal) as avg_acctbal"})
          .localPartition({})
          .finalAggregation()
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kCustomer,
              customerSelectedRowTypeWithKey,
              customerFileColumnsWithKey,
              {},
              phoneFilter)
          .capturePlanNodeId(customerScanNodeIdWithKey)
          .crossJoin(
              customerAvgAccountBalance,
              {"c_acctbal", "avg_acctbal", "c_custkey", "c_phone"})
          .filter("c_acctbal > avg_acctbal")
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              orders,
              "",
              {"c_acctbal", "c_phone"},
              core::JoinType::kNullAwareAnti)
          .project({"substr(c_phone, 1, 2) AS country_code", "c_acctbal"})
          .partialAggregation(
              {"country_code"},
              {"count(0) AS numcust", "sum(c_acctbal) AS totacctbal"})
          .localPartition({})
          .finalAggregation()
          .orderBy({"country_code"}, false)
          .planNode();

  BossPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[customerScanNodeIdWithKey] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> BossQueryBuilder::kTableNames_ = {
    kLineitem,
    kOrders,
    kCustomer,
    kNation,
    kRegion,
    kPart,
    kSupplier,
    kPartsupp};

const std::unordered_map<std::string, std::vector<std::string>>
    BossQueryBuilder::kTables_ = {
        std::make_pair(
            "lineitem",
            tpch::getTableSchema(tpch::Table::TBL_LINEITEM)->names()),
        std::make_pair(
            "orders",
            tpch::getTableSchema(tpch::Table::TBL_ORDERS)->names()),
        std::make_pair(
            "customer",
            tpch::getTableSchema(tpch::Table::TBL_CUSTOMER)->names()),
        std::make_pair(
            "nation",
            tpch::getTableSchema(tpch::Table::TBL_NATION)->names()),
        std::make_pair(
            "region",
            tpch::getTableSchema(tpch::Table::TBL_REGION)->names()),
        std::make_pair(
            "part",
            tpch::getTableSchema(tpch::Table::TBL_PART)->names()),
        std::make_pair(
            "supplier",
            tpch::getTableSchema(tpch::Table::TBL_SUPPLIER)->names()),
        std::make_pair(
            "partsupp",
            tpch::getTableSchema(tpch::Table::TBL_PARTSUPP)->names())};


std::vector<std::unordered_map<std::string, std::string>>
BossQueryBuilder::getFileColumnNamesMap(std::vector<FormExpr> &veloxExprList) {
  std::vector<std::unordered_map<std::string, std::string>> columnAliaseList;
  for (int i = 0; i < veloxExprList.size(); i++) {
    const auto &fileColumnNames = getFileColumnNames(veloxExprList[i].tableName);
    columnAliaseList.emplace_back(fileColumnNames);
  }
  return columnAliaseList;
}

void BossQueryBuilder::reformVeloxExpr(std::vector<FormExpr> &veloxExprList,
                                       std::vector<std::unordered_map<std::string, std::string>> columnAliaseList) {
  for (int i = 0; i < veloxExprList.size(); i++) {
    for (auto it = veloxExprList[i].selectedColumns.begin(); it != veloxExprList[i].selectedColumns.end();) {
      auto name = *it;
      bool resizeFlag = false;
      for (int j = 0; j < columnAliaseList.size(); j++) {
        auto idx = columnAliaseList[j].find(name);
        if (idx != columnAliaseList[j].end() && j != i) { // remove column name in the wrong table planBuilder
          if (std::find(veloxExprList[j].selectedColumns.begin(), veloxExprList[j].selectedColumns.end(), name) ==
              veloxExprList[j].selectedColumns.end())
            veloxExprList[j].selectedColumns.push_back(name);
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
      if (!resizeFlag)
        ++it;
    }
  }
}

BossPlan BossQueryBuilder::getVeloxPlanBuilder(std::vector<FormExpr> veloxExprList,
                                               std::vector<std::unordered_map<std::string, std::string>> columnAliaseList) {
  BossPlan context;
  core::PlanNodePtr planPtr;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<core::PlanNodePtr> tableMapPlan;
  std::unordered_map<std::string, int> joinMapPlan;
  std::vector<std::string> outputLayout;
  for (auto itExpr = veloxExprList.begin(); itExpr != veloxExprList.end(); ++itExpr) {
    auto &veloxExpr = *itExpr;

    const auto selectedRowType = getRowType(veloxExpr.tableName, veloxExpr.selectedColumns);
    const auto &fileColumnNames = getFileColumnNames(veloxExpr.tableName);

    core::PlanNodeId tablePlanNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
            .tableScan(veloxExpr.tableName, selectedRowType, fileColumnNames, veloxExpr.fieldFiltersVec,
                       veloxExpr.remainingFilter)
            .capturePlanNodeId(tablePlanNodeId);

    // list join first
    if (!veloxExpr.hashJoinListVec.empty()) {
      for (auto itJoin = veloxExpr.hashJoinListVec.begin(); itJoin != veloxExpr.hashJoinListVec.end(); ++itJoin) {
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
    if (itExpr == veloxExprList.end() - 1) {
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
    planPtr = plan.planNode();
    tableMapPlan.emplace_back(planPtr);
    context.dataFiles[tablePlanNodeId] = getTableFilePaths(veloxExpr.tableName);
  }
  context.plan = std::move(planPtr);
  context.dataFileFormat = format_;
  return context;
}

} // namespace facebook::velox::exec::test
