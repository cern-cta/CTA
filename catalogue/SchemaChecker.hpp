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
#include "rdbms/Login.hpp"
#include "rdbms/Conn.hpp"
#include "SchemaComparer.hpp"
#include "CatalogueSchema.hpp"
#include "SchemaCheckerResult.hpp"

namespace cta{
namespace catalogue{
    
/**
 * This class allows to check the catalogue schema validity running in a specific database
 * The database is identified thanks to its connection
 * 
 * The checking of the database consists of 
 * 1. comparing the schema running in the database with the one
 * defined in the catalogue/schema_version directory
 * 2. checking if the database contains tables that have been set as PARALLEL
 * 
 * The comparing part of the checking is done by a SchemaComparer class that will tell the differences between
 * the schema defined by the user and the one in the catalogue database.
 */  
class SchemaChecker {
public:
  
  /**
   * Destructor
   */
  virtual ~SchemaChecker();
  /**
   * Compare the schema by using a SchemaComparer and output what it is doing and errors
   * @throws Exception if no SchemaComparer has been set.
   * @return a SchemaCheckerResult
   */
  SchemaCheckerResult displayingCompareSchema(std::ostream & stdout, std::ostream & stderr);
  
  /**
   * Checks if the catalogue database contains PARALLEL tables
   * It will display a warning with the table name if this is the case
   * @return a SchemaCheckerResult
   */
  SchemaCheckerResult checkNoParallelTables();
  
  /**
   * Checks if the catalogue database schema is upgrading or not
   */
  SchemaCheckerResult checkSchemaNotUpgrading();
  
  /**
   * Compare the schema tables whose names are located in the tableList parameter
   * @param tableList the table names we would like to compare
   * @return a Status OK or FAILURE
   */
  SchemaCheckerResult compareTablesLocatedInSchema();
  
  SchemaCheckerResult checkTableContainsColumns(const std::string &tableName, const std::list<std::string> columnNames);
  
  class Builder {
  public:
    Builder(const std::string databaseToCheckName, const cta::rdbms::Login::DbType dbType, cta::rdbms::Conn &conn);
    Builder & useSQLiteSchemaComparer();
    Builder & useDirectorySchemaReader(const std::string &allSchemasVersionsDirectory);
    Builder & useMapStatementsReader();
    Builder & useCppSchemaStatementsReader(const cta::catalogue::CatalogueSchema schema);
    std::unique_ptr<SchemaChecker> build();
  private:
    const std::string m_databaseToCheckName;
    cta::rdbms::Login::DbType m_dbType;
    cta::rdbms::Conn &m_catalogueConn;
    std::unique_ptr<SchemaComparer> m_schemaComparer;
    std::unique_ptr<DatabaseMetadataGetter> m_databaseMetadataGetter;
    std::unique_ptr<SchemaSqlStatementsReader> m_schemaSqlStatementsReader;
  };
  
private:
  /**
   * Constructor of the SchemaChecker class
   * @param dbType the type of the database to check against
   * @param conn the connection of the database to check
   */
  SchemaChecker(const std::string databaseToCheckName, rdbms::Login::DbType dbType,cta::rdbms::Conn &conn);
  
  const std::string m_databaseToCheckName;
  /**
   * Catalogue-to-check database type
   */
  cta::rdbms::Login::DbType m_dbType;
  /**
   * Catalogue-to-check database connection
   */
  cta::rdbms::Conn &m_catalogueConn;
  /**
   * SchemaComparer class to compare the database schema
   */
  std::unique_ptr<SchemaComparer> m_schemaComparer;
  
  /**
   * Catalogue metadata getter that allows to query the
   * metadatas of the catalogue database
   */
  std::unique_ptr<DatabaseMetadataGetter> m_databaseMetadataGetter;
  
  void checkSchemaComparerNotNull(const std::string & methodName);
};

}}


