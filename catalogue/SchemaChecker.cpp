/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SchemaChecker.hpp"

#include "SQLiteSchemaComparer.hpp"

namespace cta::catalogue {

SchemaChecker::SchemaChecker(const std::string& databaseToCheckName,
                             rdbms::Login::DbType dbType,
                             cta::rdbms::Conn& conn)
    : m_databaseToCheckName(databaseToCheckName),
      m_dbType(dbType),
      m_catalogueConn(conn) {
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType, m_catalogueConn));
}

void SchemaChecker::checkSchemaComparerNotNull(const std::string& methodName) {
  if (m_schemaComparer == nullptr) {
    std::string exceptionMsg =
      "In " + methodName
      + ", No schema comparer used. Please specify the schema comparer by using the methods useXXXXSchemaComparer()";
    throw cta::exception::Exception(exceptionMsg);
  }
}

SchemaCheckerResult SchemaChecker::displayingCompareSchema(std::ostream& stdOut, std::ostream& stdErr) {
  checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  SchemaCheckerResult totalResult;
  stdOut << "Schema version : " << m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>()
         << std::endl;
  stdOut << "Checking indexes..." << std::endl;
  SchemaCheckerResult resIndex = m_schemaComparer->compareIndexes();
  totalResult += resIndex;
  resIndex.displayErrors(stdErr);
  stdOut << "  " << SchemaCheckerResult::statusToString(resIndex.getStatus()) << std::endl;
  stdOut << "Checking tables, columns and constraints..." << std::endl;
  SchemaCheckerResult resTables = m_schemaComparer->compareTables();
  totalResult += resTables;
  resTables.displayErrors(stdErr);
  stdOut << "  " << SchemaCheckerResult::statusToString(resTables.getStatus()) << std::endl;
  stdOut << "Status of the checking : " << cta::catalogue::SchemaCheckerResult::statusToString(totalResult.getStatus())
         << std::endl;
  return totalResult;
}

SchemaCheckerResult SchemaChecker::warnParallelTables() {
  SchemaCheckerResult res;
  auto parallelTables = m_databaseMetadataGetter->getParallelTableNames();
  for (const auto& table : parallelTables) {
    std::string warning = "TABLE " + table + " is set as PARALLEL";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnSchemaUpgrading() {
  SchemaCheckerResult res;

  if (SchemaVersion catalogueVersion = m_databaseMetadataGetter->getCatalogueVersion();
      catalogueVersion.getStatus<SchemaVersion::Status>() == SchemaVersion::Status::UPGRADING) {
    std::string warning = "The status of the schema is " + catalogueVersion.getStatus<std::string>()
                          + ", the future version is " + catalogueVersion.getSchemaVersionNext<std::string>();
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::compareTablesLocatedInSchema() {
  checkSchemaComparerNotNull(__PRETTY_FUNCTION__);
  return m_schemaComparer->compareTablesLocatedInSchema();
}

SchemaCheckerResult SchemaChecker::checkTableContainsColumns(const std::string& tableName,
                                                             const std::vector<std::string>& columnNames) {
  std::map<std::string, std::string, std::less<>> mapColumnsTypes = m_databaseMetadataGetter->getColumns(tableName);
  SchemaCheckerResult res;
  if (mapColumnsTypes.empty()) {
    std::string error = "TABLE " + tableName + " does not exist.";
    res.addError(error);
    return res;
  }
  for (auto& columnName : columnNames) {
    if (!mapColumnsTypes.contains(columnName)) {
      std::string error = "TABLE " + tableName + " does not contain the column " + columnName + ".";
      res.addError(error);
    }
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnProcedures() {
  SchemaCheckerResult res;
  auto procedureNames = m_databaseMetadataGetter->getStoredProcedures();
  for (const auto& procedure : procedureNames) {
    std::string warning = "PROCEDURE " + procedure + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnSynonyms() {
  SchemaCheckerResult res;
  auto synonymsNames = m_databaseMetadataGetter->getSynonyms();
  for (const auto& synonym : synonymsNames) {
    std::string warning = "SYNONYM " + synonym + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnTypes() {
  SchemaCheckerResult res;
  auto typeNames = m_databaseMetadataGetter->getTypes();
  for (const auto& type : typeNames) {
    std::string warning = "TYPE " + type + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnErrorLoggingTables() {
  SchemaCheckerResult res;
  auto errorTables = m_databaseMetadataGetter->getErrorLoggingTables();
  for (const auto& errorTable : errorTables) {
    std::string warning = "Error logging table " + errorTable + " exists in the " + m_databaseToCheckName + " database";
    res.addWarning(warning);
  }
  return res;
}

SchemaCheckerResult SchemaChecker::warnMissingIndexes() {
  SchemaCheckerResult res;
  auto missingIndexes = m_databaseMetadataGetter->getMissingIndexes();
  for (auto& missingIndex : missingIndexes) {
    std::string warning = "Column " + missingIndex + " is part of a foreign key constraint but has no index";
    res.addWarning(warning);
  }
  return res;
}

/////////////////////////////////////////
// SchemaChecker::Builder
/////////////////////////////////////////
SchemaChecker::Builder::Builder(const std::string& databaseToCheckName,
                                const cta::rdbms::Login::DbType& dbType,
                                cta::rdbms::Conn& conn)
    : m_databaseToCheckName(databaseToCheckName),
      m_dbType(dbType),
      m_catalogueConn(conn) {
  m_databaseMetadataGetter.reset(DatabaseMetadataGetterFactory::create(m_dbType, m_catalogueConn));
}

SchemaChecker::Builder& SchemaChecker::Builder::useSQLiteSchemaComparer() {
  m_schemaComparer.reset(new SQLiteSchemaComparer(m_databaseToCheckName, *m_databaseMetadataGetter));
  return *this;
}

SchemaChecker::Builder&
SchemaChecker::Builder::useDirectorySchemaReader(const std::string& allSchemasVersionsDirectory) {
  m_schemaSqlStatementsReader.reset(new DirectoryVersionsSqlStatementsReader(
    m_dbType,
    m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>(),
    allSchemasVersionsDirectory));
  return *this;
}

SchemaChecker::Builder& SchemaChecker::Builder::useMapStatementsReader() {
  m_schemaSqlStatementsReader.reset(
    new MapSqlStatementsReader(m_dbType,
                               m_databaseMetadataGetter->getCatalogueVersion().getSchemaVersion<std::string>()));
  return *this;
}

SchemaChecker::Builder&
SchemaChecker::Builder::useCppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema) {
  m_schemaSqlStatementsReader.reset(new CppSchemaStatementsReader(schema));
  return *this;
}

std::unique_ptr<SchemaChecker> SchemaChecker::Builder::build() {
  auto schemaChecker = std::make_unique<SchemaChecker>(m_databaseToCheckName, m_dbType, m_catalogueConn);
  if (m_schemaComparer != nullptr) {
    schemaChecker->m_schemaComparer = std::move(m_schemaComparer);
    schemaChecker->m_schemaComparer->setSchemaSqlStatementsReader(std::move(m_schemaSqlStatementsReader));
  }
  return schemaChecker;
}

}  // namespace cta::catalogue
