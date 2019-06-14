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

#include "catalogue/CatalogueSchema.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CatalogueSchema::CatalogueSchema(const std::string &sqlSchema): sql(sqlSchema) {
}
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CatalogueSchema::CatalogueSchema(const std::string &sqlSchema,
  const std::string &sqlSchemaTrigger): sql(sqlSchema), sql_trigger(sqlSchemaTrigger) {
}

//------------------------------------------------------------------------------
// getSchemaTableNames
//------------------------------------------------------------------------------
std::list<std::string> CatalogueSchema::getSchemaTableNames() const {  
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

//------------------------------------------------------------------------------
// getSchemaIndexNames
//------------------------------------------------------------------------------
std::list<std::string> CatalogueSchema::getSchemaIndexNames() const {  
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

//------------------------------------------------------------------------------
// getSchemaSequenceNames
//------------------------------------------------------------------------------
std::list<std::string> CatalogueSchema::getSchemaSequenceNames() const {  
  std::list<std::string> schemaSequences;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  try {
    while(std::string::npos != (findResult = sql.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sql.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        cta::utils::Regex tableSequencesRegex("CREATE SEQUENCE ([a-zA-Z_]+)");
        auto sequenceName = tableSequencesRegex.exec(sqlStmt);
        if (2 == sequenceName.size()) {
          schemaSequences.push_back(sequenceName[1].c_str());
        }
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return schemaSequences;
}

//------------------------------------------------------------------------------
// getSchemaTriggerNames
//------------------------------------------------------------------------------
std::list<std::string> CatalogueSchema::getSchemaTriggerNames() const {  
  std::list<std::string> schemaTriggers;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  try {
    while(std::string::npos != (findResult = sql_trigger.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(sql_trigger.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        cta::utils::Regex tableTriggersRegex("CREATE TRIGGER `([a-zA-Z_]+)`");
        auto triggerName = tableTriggersRegex.exec(sqlStmt);
        if (2 == triggerName.size()) {
          schemaTriggers.push_back(triggerName[1].c_str());
        }
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return schemaTriggers;
}

} // namespace catalogue
} // namespace cta
