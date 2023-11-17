/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>

#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"

namespace cta {

namespace utils {
class Regex;
}

namespace catalogue {
/**
 * Interface class to get database metadata
 */
class MetadataGetter{
protected:
  void removeObjectNameContaining(std::list<std::string>& objects, const std::list<std::string> &wordsToTriggerRemoval);
  void removeObjectNameNotContaining(std::list<std::string>& objects, const std::list<std::string> &wordsNotToTriggerRemoval);
  void removeObjectNameNotMatches(std::list<std::string> &objects, const cta::utils::Regex &regex);
  void removeObjectNameMatches(std::list<std::string> &objects, const cta::utils::Regex &regex);
public:
  virtual std::list<std::string> getIndexNames() = 0;
  virtual std::list<std::string> getTableNames() = 0;
  virtual std::map<std::string, std::string> getColumns(const std::string& tableName) = 0;
  virtual std::list<std::string> getConstraintNames(const std::string& tableName) = 0;
  virtual ~MetadataGetter() = 0;
};

/**
 * This class is used to get the Metadata (table names, columns, indexes) of the database accessed via the connection given in parameters
 * It will simply call the methods from the connection (Conn) instance and adapt or not the metadata returned.
 */
class DatabaseMetadataGetter: public MetadataGetter {
protected:
  rdbms::Conn& m_conn;
public:
  explicit DatabaseMetadataGetter(cta::rdbms::Conn & conn);
  virtual ~DatabaseMetadataGetter();
  SchemaVersion getCatalogueVersion();
  virtual std::list<std::string> getIndexNames();
  virtual std::list<std::string> getTableNames();
  virtual std::map<std::string, std::string> getColumns(const std::string& tableName);
  virtual std::list<std::string> getConstraintNames(const std::string& tableName);
  virtual std::list<std::string> getParallelTableNames();
  virtual cta::rdbms::Login::DbType getDbType() = 0;
  virtual std::list<std::string> getStoredProcedures();
  virtual std::list<std::string> getSynonyms();
  virtual std::list<std::string> getTypes();
  /**
    * Returns ERROR logging tables.
    * (Oracle only : ERR$_TABLE_NAME)
    * @return the ERROR logging tables (Oracle only)
    */
  virtual std::list<std::string> getErrorLoggingTables();
  /**
    * Returns a set of columns which are part of a foreign key constraint but have no index defined
    */
  virtual std::set<std::string> getMissingIndexes() = 0;
};

/**
 * Specific SQLite database metadata getter
 */
class SQLiteDatabaseMetadataGetter: public DatabaseMetadataGetter{
public:
  explicit SQLiteDatabaseMetadataGetter(cta::rdbms::Conn& conn);
  virtual ~SQLiteDatabaseMetadataGetter();
  std::list<std::string> getIndexNames() override;
  std::list<std::string> getTableNames() override;
  cta::rdbms::Login::DbType getDbType() override;
  std::set<std::string> getMissingIndexes() override;
};

/**
 * Specific Oracle database metadata getter
 */
class OracleDatabaseMetadataGetter: public DatabaseMetadataGetter{
public:
  explicit OracleDatabaseMetadataGetter(cta::rdbms::Conn& conn);
  virtual ~OracleDatabaseMetadataGetter();
  cta::rdbms::Login::DbType getDbType() override;
  std::list<std::string> getTableNames() override;
  std::set<std::string> getMissingIndexes() override;
};

/**
 * Specific Postgres database metadata getter
 */
class PostgresDatabaseMetadataGetter: public DatabaseMetadataGetter{
public:
  explicit PostgresDatabaseMetadataGetter(cta::rdbms::Conn& conn);
  virtual ~PostgresDatabaseMetadataGetter();
  cta::rdbms::Login::DbType getDbType() override;
  std::set<std::string> getMissingIndexes() override;
};

/**
 * Factory of MetadataGetter allowing to instanciate the correct metadata getter according to the database type given in parameter
 * @param dbType the database type in order to instanciate the correct metadata getter
 * @param conn the connection of the database to get the metadata from
 * @return the correct DatabaseMetadataGetter instance
 */
class DatabaseMetadataGetterFactory {
public:
  static DatabaseMetadataGetter* create(const rdbms::Login::DbType dbType, cta::rdbms::Conn & conn);
};

/**
 * This class is used to filter the Metadata that comes from the SQLite schema regarding the dbType we are currently checking
 */
class SchemaMetadataGetter: public MetadataGetter{
protected:
  std::unique_ptr<SQLiteDatabaseMetadataGetter> m_sqliteDatabaseMetadataGetter;
  // The database type we would like to compare the SQLite schema against (used for filtering the results)
  cta::rdbms::Login::DbType m_dbType;
public:
  SchemaMetadataGetter(std::unique_ptr<SQLiteDatabaseMetadataGetter> sqliteCatalogueMetadataGetter, const cta::rdbms::Login::DbType dbType);
  std::list<std::string> getIndexNames() override;
  std::list<std::string> getTableNames() override;
  std::map<std::string, std::string> getColumns(const std::string& tableName) override;
  std::list<std::string> getConstraintNames(const std::string& tableName) override;
};

}} // namespace cta::catalogue
