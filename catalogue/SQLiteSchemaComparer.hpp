/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once

#include "SchemaComparer.hpp"

namespace cta {
namespace catalogue {

/**
 * This class allows to compare a catalogue schema against a InMemory SQLite catalogue schema
 */
class SQLiteSchemaComparer: public SchemaComparer {
public:
  /**
   * Constructs a SQLiteSchemaComparer
   * @param databaseToCheckName the database name to compare.
   * @param catalogueMetadataGetter the catalogue metadata getter to compare the catalogue schema content with the SQLite
   * database one
   */
  SQLiteSchemaComparer(const std::string& databaseToCheckName, DatabaseMetadataGetter &catalogueMetadataGetter);
  /**
   * Compare the catalogue schema against the InMemory SQLite catalogue schema
   * @return a SchemaComparerResult object that will contain the differences if there are some
   */
  SchemaCheckerResult compareAll() override;
  SchemaCheckerResult compareIndexes() override;
  SchemaCheckerResult compareTables() override;
  SchemaCheckerResult compareTablesLocatedInSchema() override;

  virtual ~SQLiteSchemaComparer();

private:
  void insertSchemaInSQLite();
  SchemaCheckerResult compareItems(const std::string &itemType, const std::list<std::string>& itemsFromDatabase, const std::list<std::string>& itemsFromSQLite);
  SchemaCheckerResult compareTables(const std::list<std::string> &databaseTables, const std::list<std::string> &schemaTables);
  typedef std::map<std::string, std::map<std::string, std::string>> TableColumns;
  SchemaCheckerResult compareTableColumns(const TableColumns & schema1TableColumns, const std::string &schema1Type,const TableColumns & schema2TableColumns, const std::string &schema2Type);
  rdbms::Conn m_sqliteConn;
  std::unique_ptr<rdbms::ConnPool> m_sqliteConnPool;
  std::unique_ptr<SchemaMetadataGetter> m_schemaMetadataGetter;
  bool m_isSchemaInserted = false;
};

}}
