#pragma once

#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

enum ColumnType { cName, cValue };
struct AtomicExpr {
  enum ColumnType type;
  std::string data;
};
struct FiledFilter {
  std::string opName;
  std::vector<AtomicExpr> element;
};
struct FormExpr {
  int32_t limit = 0;
  bool orderBy = false;
  std::string tableName;
  std::vector<std::string> selectedColumns;
  std::vector<FiledFilter> tmpFieldFiltersVec;
  std::vector<std::string> fieldFiltersVec;
  std::vector<std::string> projectionsVec;
  std::vector<std::string> groupingKeysVec;
  std::vector<std::string> aggregatesVec;
  std::vector<std::string> orderByVec;

  void clear() {
    limit = 0;
    orderBy = false;
    tableName.clear();
    selectedColumns.clear();
    tmpFieldFiltersVec.clear();
    fieldFiltersVec.clear();
    projectionsVec.clear();
    groupingKeysVec.clear();
    aggregatesVec.clear();
    orderByVec.clear();
  }
};

/// Contains the query plan and input data files keyed on source plan node ID.
/// All data files use the same file format specified in 'dataFileFormat'.
struct BossPlan {
  core::PlanNodePtr plan;
  std::unordered_map<core::PlanNodeId, std::vector<std::string>> dataFiles;
  dwio::common::FileFormat dataFileFormat;
};

/// Contains type information, data files, and file column names for a table.
/// This information is inferred from the input data files.
/// The type names are mapped to the standard names.
/// Example: If the file has a 'returnflag' column, the corresponding type name
/// will be 'l_returnflag'. fileColumnNames store the mapping between standard
/// names and the corresponding name in the file.
struct BossTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> fileColumnNames;
};

/// Builds TPC-H queries using TPC-H data files located in the specified
/// directory. Each table data must be placed in hive-style partitioning. That
/// is, the top-level directory is expected to contain a sub-directory per table
/// name and the name of the sub-directory must match the table name. Example:
/// ls -R data/
///  customer   lineitem
///
///  data/customer:
///  customer1.parquet  customer2.parquet
///
///  data/lineitem:
///  lineitem1.parquet  lineitem2.parquet  lineitem3.parquet

/// The column names can vary. Additional columns may exist towards the end.
/// The class uses standard names (example: l_returnflag) to build TPC-H plans.
/// Since the column names in the file can vary, they are mapped to the standard
/// names. Therefore, the order of the columns in the file is important and
/// should be in the same order as in the TPC-H standard.
class BossQueryBuilder {
 public:
  explicit BossQueryBuilder(dwio::common::FileFormat format)
      : format_(format) {}

  /// Read each data file, initialize row types, and determine data paths for
  /// each table.
  /// @param dataPath path to the data files
  void initialize(const std::string& dataPath);

  /// Get the query plan for a given TPC-H query number.
  /// @param queryId TPC-H query number
  BossPlan getQueryPlan(int queryId) const;

  void reformVeloxExpr(std::vector<FormExpr> &veloxExprList) const;

  BossPlan getVeloxPlanBuilder(std::vector<FormExpr> veloxExprList) const;

  /// Get the TPC-H table names present.
  static const std::vector<std::string>& getTableNames();

 private:
  BossPlan getQ1Plan() const;
  BossPlan getQ3Plan() const;
  BossPlan getQ5Plan() const;
  BossPlan getQ6Plan() const;
  BossPlan getQ7Plan() const;
  BossPlan getQ8Plan() const;
  BossPlan getQ9Plan() const;
  BossPlan getQ10Plan() const;
  BossPlan getQ12Plan() const;
  BossPlan getQ13Plan() const;
  BossPlan getQ14Plan() const;
  BossPlan getQ15Plan() const;
  BossPlan getQ16Plan() const;
  BossPlan getQ18Plan() const;
  BossPlan getQ19Plan() const;
  BossPlan getQ22Plan() const;

  const std::vector<std::string>& getTableFilePaths(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).dataFiles;
  }

  std::shared_ptr<const RowType> getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    auto columnSelector = std::make_shared<dwio::common::ColumnSelector>(
        tableMetadata_.at(tableName).type, columnNames);
    return columnSelector->buildSelectedReordered();
  }

  const std::unordered_map<std::string, std::string>& getFileColumnNames(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).fileColumnNames;
  }

  std::unordered_map<std::string, BossTableMetadata> tableMetadata_;
  const dwio::common::FileFormat format_;
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;

  static constexpr const char* kLineitem = "lineitem";
  static constexpr const char* kCustomer = "customer";
  static constexpr const char* kOrders = "orders";
  static constexpr const char* kNation = "nation";
  static constexpr const char* kRegion = "region";
  static constexpr const char* kPart = "part";
  static constexpr const char* kSupplier = "supplier";
  static constexpr const char* kPartsupp = "partsupp";
  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
};

} // namespace facebook::velox::exec::test
