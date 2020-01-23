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
   * The status of the checking of the database
   */
  enum Status {
    OK,
    FAILURE
  };
  /**
   * Constructor of the SchemaChecker class
   * @param dbType the type of the database to check against
   * @param conn the connection of the database to check
   */
  SchemaChecker(rdbms::Login::DbType dbType,cta::rdbms::Conn &conn);
  /**
   * Destructor
   */
  virtual ~SchemaChecker();
  /**
   * Set the SQLiteSchemaComparer in order to run the schema comparison.
   * The SQLiteSchemaComparer creates a InMemory SQLite database with the
   * statements defined in the allSchemasDirectoryPath and will compare what is in SQLite with 
   * what is in the catalogue database.
   * 
   * @param allSchemaVersionsDirectoryPath the schema sql statement reader used to get the schema
   */
  void useSQLiteSchemaComparer(cta::optional<std::string> allSchemaVersionsDirectoryPath);
  /**
   * Compare the schema by using a SchemaComparer
   * @throws Exception if no SchemaComparer has been set.
   * @return a Status : OK or FAILURE
   */
  Status compareSchema();
  /**
   * Checks if the catalogue database contains PARALLEL tables
   * It will display a warning with the table name if this is the case
   */
  void checkNoParallelTables();
  
  /**
   * Checks if the catalogue database schema is upgrading or not
   */
  void checkSchemaNotUpgrading();
  
private:
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
  std::unique_ptr<CatalogueMetadataGetter> m_catalogueMetadataGetter;
};

}}


