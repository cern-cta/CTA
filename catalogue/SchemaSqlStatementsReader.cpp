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

#include <fstream>
#include <list>
#include <map>
#include <memory>
#include <string>

#include "SchemaSqlStatementsReader.hpp"
#include "SqliteCatalogueSchema.hpp"
#include "PostgresCatalogueSchema.hpp"
#ifdef SUPPORT_OCCI
#include "OracleCatalogueSchema.hpp"
#endif
#include "common/exception/Exception.hpp"
#include "common/exception/NoSupportedDB.hpp"
#include "common/utils/utils.hpp"

#include "AllCatalogueSchema.hpp"

namespace cta::catalogue {

//////////////////////////////////////////////////////////////////
// SchemaSqlStatementsReader
//////////////////////////////////////////////////////////////////
std::list<std::string> SchemaSqlStatementsReader::getStatements() {
  std::unique_ptr<CatalogueSchema> schema;
  switch (m_dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
    case rdbms::Login::DBTYPE_SQLITE:
      schema.reset(new SqliteCatalogueSchema);
      break;
    case rdbms::Login::DBTYPE_POSTGRESQL:
      schema.reset(new PostgresCatalogueSchema);
      break;
    case rdbms::Login::DBTYPE_ORACLE:
#ifdef SUPPORT_OCCI
      schema.reset(new OracleCatalogueSchema);
#else
      throw exception::NoSupportedDB("Oracle Catalogue Schema is not supported. Compile CTA with Oracle support.");
#endif
      break;
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("Cannot get statements without a database type");
    }
    return getAllStatementsFromSchema(schema->sql);
}

std::list<std::string> SchemaSqlStatementsReader::getAllStatementsFromSchema(const std::string& schema) {
  std::list<std::string> statements;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  try {
    while (std::string::npos != (findResult = schema.find(';', searchPos))) {
      // Calculate the length of the current statement without the trailing ';'
      const std::string::size_type stmtLen = findResult - searchPos;
      const std::string sqlStmt = utils::trimString(schema.substr(searchPos, stmtLen));
      searchPos = findResult + 1;

      if (0 < sqlStmt.size()) {  // Ignore empty statements
        statements.push_back(sqlStmt+";");
      }
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
  return statements;
}

std::string SchemaSqlStatementsReader::getDatabaseType() {
  switch (m_dbType) {
    case rdbms::Login::DBTYPE_IN_MEMORY:
    case rdbms::Login::DBTYPE_SQLITE:
      return "sqlite";
    case rdbms::Login::DBTYPE_POSTGRESQL:
      return "postgres";
#ifdef SUPPORT_OCCI
    case rdbms::Login::DBTYPE_ORACLE:
      return "oracle";
#endif
    case rdbms::Login::DBTYPE_NONE:
      throw exception::Exception("The database type should not be DBTYPE_NONE");
    default:
    {
      exception::Exception ex;
      ex.getMessage() << "Unknown database type: value=" << m_dbType;
      throw ex;
    }
  }
}

//////////////////////////////////////////////////////////////////
// DirectoryVersionsSqlStatementsReader
//////////////////////////////////////////////////////////////////
DirectoryVersionsSqlStatementsReader::DirectoryVersionsSqlStatementsReader(const cta::rdbms::Login::DbType dbType,
  const std::string &catalogueVersion, const std::string &allSchemasVersionPath)
  : SchemaSqlStatementsReader(dbType), m_catalogueVersion(catalogueVersion), m_allSchemasVersionPath(allSchemasVersionPath) {
}

DirectoryVersionsSqlStatementsReader::DirectoryVersionsSqlStatementsReader(const DirectoryVersionsSqlStatementsReader& orig)
  : SchemaSqlStatementsReader(orig), m_catalogueVersion(orig.m_catalogueVersion),
  m_allSchemasVersionPath(orig.m_allSchemasVersionPath) {
}

std::list<std::string> DirectoryVersionsSqlStatementsReader::getStatements() {
  return getAllStatementsFromSchema(readSchemaFromFile());
}

std::string DirectoryVersionsSqlStatementsReader::readSchemaFromFile() {
  std::string schemaFilePath = getSchemaFilePath();
  std::ifstream schemaFile(schemaFilePath);
  if (schemaFile.fail()) {
    throw cta::exception::Exception("In DirectoryVersionsSqlStatementsReader::readSchemaFromFile(), unable to open the file located in "
      + schemaFilePath);
  }
  std::string content((std::istreambuf_iterator<char>(schemaFile)), (std::istreambuf_iterator<char>()));
  return content;
}

std::string DirectoryVersionsSqlStatementsReader::getSchemaFilePath() {
  return m_allSchemasVersionPath+m_catalogueVersion+"/"+getDatabaseType()+c_catalogueFileNameTrailer;
}

//////////////////////////////////////////////////////////////////
// MapSqlStatementsReader
//////////////////////////////////////////////////////////////////
std::list<std::string> MapSqlStatementsReader::getStatements() {
  std::map<std::string, std::string> mapVersionSchemas;
  try {
    mapVersionSchemas = AllCatalogueSchema::mapSchema.at(m_catalogueVersion);
  } catch (const std::out_of_range &ex) {
    throw cta::exception::Exception("No schema has been found for version number "+m_catalogueVersion);
  }
  try {
    std::string schema = mapVersionSchemas.at(getDatabaseType());
    return getAllStatementsFromSchema(schema);
  } catch (const std::out_of_range &ex) {
    throw cta::exception::Exception("No schema has been found for database type "+getDatabaseType());
  }
}

//////////////////////////////////////////////////////////////////
// CppSchemaStatementsReader
//////////////////////////////////////////////////////////////////

CppSchemaStatementsReader::CppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema):m_schema(schema) {}

std::list<std::string> CppSchemaStatementsReader::getStatements() {
  return getAllStatementsFromSchema(m_schema.sql);
}

} // namespace cta::catalogue
