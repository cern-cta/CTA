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
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {
  /**
   * This class is used to create a InMemory SQLiteSchema from .sql files 
   * located in allSchemasVersionPath (example : /catalogue/1.0/dbtype_catalogue_schema.sql)
   * 
   * @param schemaVersion the schema version to insert in order to look in the correct folder within allSchemasVersionPath
   * @param catalogueDbType the dbType of the schema to insert in the SQLiteSchema
   * @param allSchemasVersionPath the directory containing all the versions of the catalogue schema
   * @param sqliteConn the connection of the InMemory SQLite schema
   */
  class SQLiteSchemaInserter {
  public:
    /**
     * Constructor
     * @param schemaVersion the schema version to insert in order to look in the correct folder within allSchemasVersionPath
     * @param catalogueDbType the dbType of the schema to insert in the SQLiteSchema
     * @param allSchemasVersionPath the directory containing all the versions of the catalogue schema
     * @param sqliteConn the connection of the InMemory SQLite schema
     */
    SQLiteSchemaInserter(const std::string & schemaVersion, const cta::rdbms::Login::DbType &catalogueDbType, const std::string &allSchemasVersionPath,rdbms::Conn &sqliteConn);
    /**
     * Insert the schema corresponding to the version and the database type into the
     * InMemory SQLite database accessible via the one given in the constructor
     */
    void insert();    
    virtual ~SQLiteSchemaInserter();
  private:
    const std::string c_catalogueFileNameTrailer = "_catalogue_schema.sql";
    
    std::string m_schemaVersion;
    cta::rdbms::Login::DbType m_dbType;
    std::string m_allSchemasVersionPath;
    cta::rdbms::Conn & m_sqliteCatalogueConn;
    /**
     * Put the schema located in SCHEMA_VERSION/dbType_catalogue_schema.sql
     * @return the string containing the .sql statements for the creation of the schema
     */
    std::string readSchemaFromFile();
    /**
     * Separates the statements and put them in a std::list<std::string> 
     * @param schema the sql statements put all together
     * @return a list containing separated statements from the schema passed in parameter
     */
    std::list<std::string> getAllStatementsFromSchema(const std::string &schema);
    /**
     * Returns the string corresponding to the database type
     * This method is used to determine the correct .sql file to create the InMemory SQLite database
     * @return the string corresponding to the database type
     */
    std::string getDatabaseType();
    /**
     * Returns the path of the .sql file that will be executed to create the InMemory SQLite database
     * @return 
     */
    std::string getSchemaFilePath();
    /**
     * Execute all the statements passed in parameter against the InMemory SQLite database 
     * @param statements the statements to execute in the InMemory SQLite database
     */
    void executeStatements(const std::list<std::string> &statements);
  };
  
}}


