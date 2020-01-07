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
#include <list>

namespace cta {
namespace catalogue {

/**
 * This class is used to get the Metadata (table names, columns, indexes) of the database accessed via the connection given in parameters
 * It will simply call the methods from the connection (Conn) instance and adapt or not the metadata returned.  
 */  
class CatalogueMetadataGetter {
  protected:
    rdbms::Conn& m_conn;
    void removeObjectNameContaining(std::list<std::string>& objects, const std::list<std::string> &wordsToTriggerRemoval);
    void removeObjectNameNotContaining(std::list<std::string>& objects, const std::list<std::string> &wordsNotToTriggerRemoval);
    void removeObjectNameNotMatches(std::list<std::string> &objects, const cta::utils::Regex &regex);
    void removeObjectNameMatches(std::list<std::string> &objects, const cta::utils::Regex &regex);
  public:
    CatalogueMetadataGetter(cta::rdbms::Conn & conn);
    virtual ~CatalogueMetadataGetter();
    std::string getCatalogueVersion();
    virtual std::list<std::string> getIndexNames();
    virtual std::list<std::string> getTableNames();
    virtual std::map<std::string,std::string> getColumns(const std::string& tableName);
    virtual std::list<std::string> getConstraintNames(const std::string& tableName);
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
  std::map<std::string,std::string> getColumns(const std::string& tableName) override;
  std::list<std::string> getConstraintNames(const std::string &tableName) override;
  /**
   * Get the constraint names but filters the result to be compatible with the database type passed in parameter
   * @param tableName the table for which we want the constraint
   * @param dbType the database type for which the results will be compatible
   * @return  the constraint names filtered in order to be compatible with the database type passed in parameter
   */
  std::list<std::string> getConstraintNames(const std::string &tableName, cta::rdbms::Login::DbType dbType);
};

/**
 * Specific Oracle Catalogue metadata getter
 */
class OracleCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
  OracleCatalogueMetadataGetter(cta::rdbms::Conn & conn);
  virtual ~OracleCatalogueMetadataGetter();
  std::list<std::string> getIndexNames() override;
  std::list<std::string> getTableNames() override;
  std::map<std::string,std::string> getColumns(const std::string& tableName) override;
  std::list<std::string> getConstraintNames(const std::string &tableName) override;
};

/**
 * Specific MySQL Catalogue metadata getter
 */
class MySQLCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
    MySQLCatalogueMetadataGetter(cta::rdbms::Conn &conn);
    virtual ~MySQLCatalogueMetadataGetter();
    std::list<std::string> getIndexNames() override;
    std::list<std::string> getTableNames() override;
    std::map<std::string,std::string> getColumns(const std::string& tableName) override;
    std::list<std::string> getConstraintNames(const std::string &tableName) override;
};

/**
 * Specific Postgres Catalogue metadata getter
 */
class PostgresCatalogueMetadataGetter: public CatalogueMetadataGetter{
  public:
    PostgresCatalogueMetadataGetter(cta::rdbms::Conn &conn);
    virtual ~PostgresCatalogueMetadataGetter();
    std::list<std::string> getIndexNames() override;
    std::list<std::string> getTableNames() override;
    std::map<std::string,std::string> getColumns(const std::string& tableName) override;
    std::list<std::string> getConstraintNames(const std::string &tableName) override;
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

}}