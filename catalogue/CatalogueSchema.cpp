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

#include "catalogue/CatalogueSchema.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CatalogueSchema::CatalogueSchema(): sql("") {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CatalogueSchema::CatalogueSchema(const std::string &sqlSchema): sql(sqlSchema) {
}

//------------------------------------------------------------------------------
// getSchemaColumns
//------------------------------------------------------------------------------
std::map<std::string, std::string, std::less<>> CatalogueSchema::getSchemaColumns(const std::string &tableName) const {
  std::map<std::string, std::string, std::less<>> schemaColumnNames;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  const std::string columnTypes = 
    "NUMERIC|"
    "INTEGER|"
    "CHAR|"
    "VARCHAR|"
    "VARCHAR2|"
    "BLOB|"
    "BYTEA|"
    "VARBINARY|"
    "RAW";
  
  try {
    while(std::string::npos != (findResult = sql.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(std::string_view(sql).substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if (sqlStmt.empty()) {
        // Ignore empty statements
        continue;
      }
      const std::string createTableSQL = "CREATE[a-zA-Z ]+TABLE " + tableName + "[ ]*\\(([a-zA-Z0-9_, '\\)\\(]+)\\)";
      cta::utils::Regex tableSqlRegex(createTableSQL.c_str());
      auto tableSql = tableSqlRegex.exec(sqlStmt);
      if (tableSql.size() != 2) {
        // Ensure the dimensions are correct
        continue;
      }
      tableSql[1] += ",";  // hack for parsing
      // we use the same logic here as for trailing ';'
      std::string::size_type searchPosComma = 0;
      std::string::size_type findResultComma = std::string::npos;
      while (std::string::npos != (findResultComma = tableSql[1].find(',', searchPosComma))) {
        const std::string::size_type stmtLenComma = findResultComma - searchPosComma;
        const std::string sqlStmtComma =
          utils::trimString(std::string_view(tableSql[1]).substr(searchPosComma, stmtLenComma));
        searchPosComma = findResultComma + 1;

        if (sqlStmtComma.empty()) {
          // Ignore empty statements
          continue;
        }
        const std::string columnSQL = "([a-zA-Z_0-9]+) +(" + columnTypes + ")";
        cta::utils::Regex columnSqlRegex(columnSQL.c_str());
        auto columnSql = columnSqlRegex.exec(sqlStmtComma);
        if (3 == columnSql.size()) {
          schemaColumnNames.try_emplace(columnSql[1], columnSql[2]);
        }
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return schemaColumnNames;
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
      const std::string sqlStmt = utils::trimString(std::string_view(sql).substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        cta::utils::Regex tableNamesRegex("CREATE[a-zA-Z ]+TABLE ([a-zA-Z_0-9]+)");
        auto tableName = tableNamesRegex.exec(sqlStmt);
        if (2 == tableName.size()) {
          schemaTables.push_back(tableName[1]);
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
      const std::string sqlStmt = utils::trimString(std::string_view(sql).substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        cta::utils::Regex tableIndicesRegex("CREATE INDEX ([a-zA-Z_]+)");
        auto indexName = tableIndicesRegex.exec(sqlStmt);
        if (2 == indexName.size()) {
          schemaIndices.emplace_back(indexName[1].c_str());
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
      const std::string sqlStmt = utils::trimString(std::string_view(sql).substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if(0 < sqlStmt.size()) { // Ignore empty statements
        cta::utils::Regex tableSequencesRegex("CREATE SEQUENCE ([a-zA-Z_]+)");
        auto sequenceName = tableSequencesRegex.exec(sqlStmt);
        if (2 == sequenceName.size()) {
          schemaSequences.emplace_back(sequenceName[1].c_str());
        }
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return schemaSequences;
}

//------------------------------------------------------------------------------
// getSchemaVersion
//------------------------------------------------------------------------------
std::map<std::string, uint64_t, std::less<>> CatalogueSchema::getSchemaVersion() const {
  try {
    std::map<std::string, uint64_t, std::less<>> schemaVersion;
    cta::utils::Regex schemaVersionRegex(
      "INSERT INTO CTA_CATALOGUE\\("
      "  SCHEMA_VERSION_MAJOR,"
      "  SCHEMA_VERSION_MINOR\\)"
      "VALUES\\("
      "  ([[:digit:]]+),"
      "  ([[:digit:]]+)\\);"
    );
    if (auto version = schemaVersionRegex.exec(sql); version.size() == 3) {
      schemaVersion.try_emplace("SCHEMA_VERSION_MAJOR", cta::utils::toUint64(version[1].c_str()));
      schemaVersion.try_emplace("SCHEMA_VERSION_MINOR", cta::utils::toUint64(version[2].c_str()));
    } else {
      exception::Exception ex;
      ex.getMessage() << "Could not find SCHEMA_VERSION";
      throw ex; 
    }
    return schemaVersion;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace cta::catalogue
