/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "rdbms/Conn.hpp"
#include "rdbms/Login.hpp"
#include "SchemaCreatingSqliteCatalogue.hpp"
#include "SchemaVersion.hpp"
#include <list>

namespace cta {
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
  virtual std::map<std::string,std::string> getColumns(const std::string& tableName) = 0;
  virtual std::list<std::string> getConstraintNames(const std::string& tableName) = 0;
  virtual ~MetadataGetter() = 0;
};

/**
 * This class is used to get the Metadata (table names, columns, indexes) of the database accessed via the connection given in parameters
 * It will simply call the methods from the connection (Conn) instance and adapt or not the metadata returned.  
 */  
class CatalogueMetadataGetter: public MetadataGetter {
  protected:
    rdbms::Conn& m_conn;
  public:
    CatalogueMetadataGetter(cta::rdbms::Conn & conn);
    virtual ~CatalogueMetadataGetter();
    SchemaVersion getCatalogueVersion();
    virtual std::list<std::string> getIndexNames();
    virtual std::list<std::string> getTableNames();
    virtual std::map<std::string,std::string> getColumns(const std::string& tableName);
    virtual std::list<std::string> getConstraintNames(const std::string& tableName);
    virtual std::list<std::string> getParallelTableNames();
    virtual cta::rdbms::Login::DbType getDbType() = 0;
};

/**
 * Specific SQLite Catalogue metadata getter
 */
class SQLiteCatalogueMetadataGetter: public CatalogueMetadataGetter{
public:
  SQLiteCatalogueMetadataGetter(cta::rdbms::Conn & conn);
  virtual ~SQLiteCatalogueMetadataGetter();
  std::list<std::string> getIndexNames() override;
  std::list<std::string> getTableNames() override;
  cta::rdbms::Login::DbType getDbType() override;
};

/**
 * Specific Oracle Catalogue metadata getter
 */
class OracleCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
  OracleCatalogueMetadataGetter(cta::rdbms::Conn & conn);
  cta::rdbms::Login::DbType getDbType() override;
  virtual ~OracleCatalogueMetadataGetter();
};

/**
 * Specific MySQL Catalogue metadata getter
 */
class MySQLCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
    MySQLCatalogueMetadataGetter(cta::rdbms::Conn &conn);
    cta::rdbms::Login::DbType getDbType() override;
    virtual ~MySQLCatalogueMetadataGetter();
};

/**
 * Specific Postgres Catalogue metadata getter
 */
class PostgresCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
    PostgresCatalogueMetadataGetter(cta::rdbms::Conn &conn);
    cta::rdbms::Login::DbType getDbType() override;
    virtual ~PostgresCatalogueMetadataGetter();
};

/**
 * Factory of MetadataGetter allowing to instanciate the correct metadata getter according to the database type given in parameter
 * @param dbType the database type in order to instanciate the correct metadata getter
 * @param conn the connection of the database to get the metadata from
 * @return the correct CatalogueMetadataGetter instance
 */
class CatalogueMetadataGetterFactory {
public:
  static CatalogueMetadataGetter* create(const rdbms::Login::DbType dbType, cta::rdbms::Conn & conn);
};

/**
 * This class is used to filter the Metadata that comes from the SQLite schema regarding the dbType we are currently checking
 */
class SchemaMetadataGetter: public MetadataGetter{
protected:
  std::unique_ptr<SQLiteCatalogueMetadataGetter> m_sqliteCatalogueMetadataGetter;
  //The database type we would like to compare the SQLite schema against (used for filtering the results)
  cta::rdbms::Login::DbType m_dbType;
public:
  SchemaMetadataGetter(std::unique_ptr<SQLiteCatalogueMetadataGetter> sqliteCatalogueMetadataGetter, const cta::rdbms::Login::DbType dbType);
  virtual std::list<std::string> getIndexNames() override;
  virtual std::list<std::string> getTableNames() override;
  virtual std::map<std::string,std::string> getColumns(const std::string& tableName) override;
  virtual std::list<std::string> getConstraintNames(const std::string& tableName) override;
};

}}