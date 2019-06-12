/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

#include <string>
#include <list>

// TODO
#include <iostream>

namespace cta {
namespace catalogue {

/**
 * Structure containing the SQL to create the schema of the CTA catalogue
 * database in an SQLite database.
 *
 * The CMakeLists.txt file of this directory instructs cmake to generate
 * SqliteCatalogueSchema.cpp from:
 *   - SqliteCatalogueSchema.before_SQL.cpp
 *   - sqlite_catalogue_schema.sql
 *
 * The SqliteCatalogueSchema.before_SQL.cpp file is not compilable and is therefore
 * difficult for Integrated Developent Environments (IDEs) to handle.
 *
 * The purpose of this class is to help IDEs by isolating the "non-compilable"
 * issues into a small cpp file.
 */
struct SqliteCatalogueSchema {
  /**
   * Constructor.
   */
  SqliteCatalogueSchema();

  /**
   * The schema.
   */
  const std::string sql;
  // TODO
  std::list<std::string> getSchemaTableNames() const {  
    std::list<std::string> schemaTables;
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;
    try {
      while(std::string::npos != (findResult = sql.find(';', searchPos))) {
        // Calculate the length of the current statement without the trailing ';'
        const std::string::size_type stmtLen = findResult - searchPos;
        const std::string sqlStmt = utils::trimString(sql.substr(searchPos, stmtLen));
        searchPos = findResult + 1;
        
        if(0 < sqlStmt.size()) { // Ignore empty statements
          cta::utils::Regex tableNamesRegex("CREATE TABLE ([a-zA-Z_]+)");
          auto tableName = tableNamesRegex.exec(sqlStmt);
          if (2 == tableName.size()) {
            schemaTables.push_back(tableName[1].c_str());
          }
        }
      }
    } catch(exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    }
    return schemaTables;
  }
  // TODO
  std::list<std::string> getSchemaIndexNames() {  
    std::list<std::string> schemaIndices;
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;
    try {
      while(std::string::npos != (findResult = sql.find(';', searchPos))) {
        // Calculate the length of the current statement without the trailing ';'
        const std::string::size_type stmtLen = findResult - searchPos;
        const std::string sqlStmt = utils::trimString(sql.substr(searchPos, stmtLen));
        searchPos = findResult + 1;
        
        if(0 < sqlStmt.size()) { // Ignore empty statements
          cta::utils::Regex tableIndicesRegex("CREATE INDEX ([a-zA-Z_]+)");
          auto indexName = tableIndicesRegex.exec(sqlStmt);
          if (2 == indexName.size()) {
            schemaIndices.push_back(indexName[1].c_str());
          }
        }
      }
    } catch(exception::Exception &ex) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    }
    return schemaIndices;
  }
};

} // namespace catalogue
} // namespace cta
