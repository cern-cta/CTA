/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "SchemaComparer.hpp"

#include <tuple>

namespace cta::catalogue {

/**
 * This class allows to compare a catalogue schema against a InMemory SQLite catalogue schema
 */
class SQLiteSchemaComparer : public SchemaComparer {
public:
  /**
   * Constructs a SQLiteSchemaComparer
   * @param databaseToCheckName the database name to compare.
   * @param catalogueMetadataGetter the catalogue metadata getter to compare the catalogue schema content with the SQLite
   * database one
   */
  SQLiteSchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter& catalogueMetadataGetter);
  /**
   * Compare the catalogue schema against the InMemory SQLite catalogue schema
   * @return a SchemaComparerResult object that will contain the differences if there are some
   */
  SchemaCheckerResult compareAll() override;
  SchemaCheckerResult compareIndexes() override;
  SchemaCheckerResult compareTables() override;
  SchemaCheckerResult compareTablesLocatedInSchema() override;

  ~SQLiteSchemaComparer() override;

private:
  void insertSchemaInSQLite();

  enum Level { Warn, Error };

  using Items = std::vector<std::string>;
  using LoggedItems = std::tuple<Items, Level>;
  SchemaCheckerResult
  compareItems(const std::string& itemType, const LoggedItems& fromDatabase, const LoggedItems& fromSQLite) const;
  SchemaCheckerResult compareTables(const std::vector<std::string>& databaseTables,
                                    const std::vector<std::string>& schemaTables);
  using TableColumns = std::map<std::string, std::map<std::string, std::string, std::less<>>, std::less<>>;
  SchemaCheckerResult compareTableColumns(const TableColumns& schema1TableColumns,
                                          const std::string& schema1Type,
                                          const TableColumns& schema2TableColumns,
                                          const std::string& schema2Type) const;
  rdbms::Conn m_sqliteConn;
  std::unique_ptr<rdbms::ConnPool> m_sqliteConnPool;
  std::unique_ptr<SchemaMetadataGetter> m_schemaMetadataGetter;
  bool m_isSchemaInserted = false;
};

}  // namespace cta::catalogue
