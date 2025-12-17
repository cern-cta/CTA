/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SchemaSqlStatementsReader.hpp"

#include "PostgresCatalogueSchema.hpp"
#include "SqliteCatalogueSchema.hpp"

#include <fstream>
#include <list>
#include <map>
#include <memory>
#include <string>
#ifdef SUPPORT_OCCI
#include "OracleCatalogueSchema.hpp"
#endif
#include "AllCatalogueSchema.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/NoSupportedDB.hpp"
#include "common/utils/utils.hpp"

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

std::list<std::string> SchemaSqlStatementsReader::getAllStatementsFromSchema(const std::string& schema) const {
  std::list<std::string> statements;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  while (std::string::npos != (findResult = schema.find(';', searchPos))) {
    // Calculate the length of the current statement without the trailing ';'
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(std::string_view(schema).substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (0 < sqlStmt.size()) {  // Ignore empty statements
      statements.push_back(sqlStmt + ";");
    }
  }
  return statements;
}

std::string SchemaSqlStatementsReader::getDatabaseType() const {
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
    default: {
      exception::Exception ex;
      ex.getMessage() << "Unknown database type: value=" << m_dbType;
      throw ex;
    }
  }
}

//////////////////////////////////////////////////////////////////
// DirectoryVersionsSqlStatementsReader
//////////////////////////////////////////////////////////////////
std::string DirectoryVersionsSqlStatementsReader::readSchemaFromFile() const {
  std::string schemaFilePath = getSchemaFilePath();
  std::ifstream schemaFile(schemaFilePath);
  if (schemaFile.fail()) {
    throw cta::exception::Exception(
      "In DirectoryVersionsSqlStatementsReader::readSchemaFromFile(), unable to open the file located in "
      + schemaFilePath);
  }
  std::string content((std::istreambuf_iterator<char>(schemaFile)), (std::istreambuf_iterator<char>()));
  return content;
}

//////////////////////////////////////////////////////////////////
// MapSqlStatementsReader
//////////////////////////////////////////////////////////////////
std::list<std::string> MapSqlStatementsReader::getStatements() {
  std::map<std::string, std::string, std::less<>> mapVersionSchemas;
  try {
    mapVersionSchemas = AllCatalogueSchema::mapSchema.at(m_catalogueVersion);
  } catch (const std::out_of_range&) {
    throw cta::exception::Exception("No schema has been found for version number " + m_catalogueVersion);
  }
  try {
    std::string schema = mapVersionSchemas.at(getDatabaseType());
    return getAllStatementsFromSchema(schema);
  } catch (const std::out_of_range&) {
    throw cta::exception::Exception("No schema has been found for database type " + getDatabaseType());
  }
}

//////////////////////////////////////////////////////////////////
// CppSchemaStatementsReader
//////////////////////////////////////////////////////////////////

CppSchemaStatementsReader::CppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema)
    : m_schema(schema) {}

std::list<std::string> CppSchemaStatementsReader::getStatements() {
  return getAllStatementsFromSchema(m_schema.sql);
}

}  // namespace cta::catalogue
