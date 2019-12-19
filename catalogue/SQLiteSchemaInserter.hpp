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
  
  class SQLiteSchemaInserter {
  public:
    SQLiteSchemaInserter(const std::string & schemaVersion, const cta::rdbms::Login::DbType &catalogueDbType, const std::string &allSchemasVersionPath,rdbms::Conn &sqliteConn);
    void insert();    
    virtual ~SQLiteSchemaInserter();
  private:
    const std::string c_catalogueFileNameTrailer = "_catalogue_schema.sql";
    
    std::string m_schemaVersion;
    cta::rdbms::Login::DbType m_dbType;
    std::string m_allSchemasVersionPath;
    cta::rdbms::Conn & m_sqliteCatalogueConn;
    std::string readSchemaFromFile();
    std::list<std::string> getAllStatementsFromSchema(const std::string &schema);
    std::string getDatabaseType();
    std::string getSchemaFilePath();
    void executeStatements(const std::list<std::string> &statements);
  };
  
}}


