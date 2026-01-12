/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/DatabaseMetadataGetter.hpp"

#include "common/utils/Regex.hpp"

#include <algorithm>

namespace cta::catalogue {

// Default destructor for abstract base class
MetadataGetter::~MetadataGetter() = default;

void MetadataGetter::removeObjectNameContaining(std::vector<std::string>& objects,
                                                std::span<const std::string_view> wordsToTriggerRemoval) const {
  std::erase_if(objects, [&wordsToTriggerRemoval](const std::string& object) {
    return std::ranges::find(wordsToTriggerRemoval, object) != wordsToTriggerRemoval.end();
  });
}

void MetadataGetter::removeObjectNameNotContaining(std::vector<std::string>& objects,
                                                   std::span<const std::string_view> wordsNotToTriggerRemoval) const {
  std::erase_if(objects, [&wordsNotToTriggerRemoval](const std::string& object) {
    return std::ranges::find(wordsNotToTriggerRemoval, object) == wordsNotToTriggerRemoval.end();
  });
}

void MetadataGetter::removeObjectNameNotMatches(std::vector<std::string>& objects,
                                                const cta::utils::Regex& regex) const {
  std::erase_if(objects, [&regex](const std::string& object) { return !regex.has_match(object); });
}

void MetadataGetter::removeObjectNameMatches(std::vector<std::string>& objects, const cta::utils::Regex& regex) const {
  std::erase_if(objects, [&regex](const std::string& object) { return regex.has_match(object); });
}

DatabaseMetadataGetter::DatabaseMetadataGetter(cta::rdbms::Conn& conn) : m_conn(conn) {}

SchemaVersion DatabaseMetadataGetter::getCatalogueVersion() {
  const char* const sql = R"SQL(
    SELECT
      CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,
      CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR
    FROM
      CTA_CATALOGUE
  )SQL";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  if (rset.next()) {
    SchemaVersion::Builder schemaVersionBuilder;
    schemaVersionBuilder.schemaVersionMajor(rset.columnUint64("SCHEMA_VERSION_MAJOR"))
      .schemaVersionMinor(rset.columnUint64("SCHEMA_VERSION_MINOR"))
      //By default, the status is set as PRODUCTION (to be backward-compatible with version 1.0 of the schema)
      .status(SchemaVersion::Status::PRODUCTION);

    //The cta-catalogue-schema-verify tool has to be backward-compatible with version 1.0
    //of the schema that does not have the NEXT_SCHEMA_VERSION_MAJOR, NEXT_SCHEMA_VERSION_MINOR and the STATUS column
    const char* const sql2 = R"SQL(
      SELECT
        CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MAJOR AS NEXT_SCHEMA_VERSION_MAJOR,
        CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MINOR AS NEXT_SCHEMA_VERSION_MINOR,
        CTA_CATALOGUE.STATUS AS STATUS
      FROM
        CTA_CATALOGUE
    )SQL";

    auto stmt2 = m_conn.createStmt(sql2);
    try {
      auto rset2 = stmt2.executeQuery();
      if (rset2.next()) {
        auto schemaStatus = rset2.columnString("STATUS");
        {
          auto schemaVersionMajorNext = rset2.columnOptionalUint64("NEXT_SCHEMA_VERSION_MAJOR");
          auto schemaVersionMinorNext = rset2.columnOptionalUint64("NEXT_SCHEMA_VERSION_MINOR");
          if (schemaVersionMajorNext.has_value() && schemaVersionMinorNext.has_value()) {
            schemaVersionBuilder.nextSchemaVersionMajor(schemaVersionMajorNext.value())
              .nextSchemaVersionMinor(schemaVersionMinorNext.value())
              .status(schemaStatus);
          }
        }
      }
    } catch (const cta::exception::Exception&) {}
    return schemaVersionBuilder.build();
  } else {
    throw exception::Exception("CTA_CATALOGUE does not contain any row");
  }
}

std::vector<std::string> DatabaseMetadataGetter::getTableNames() {
  static constexpr std::array<std::string_view, 2> toRemove = {"DATABASECHANGELOG", "DATABASECHANGELOGLOCK"};
  auto tableNames = m_conn.getTableNames();
  removeObjectNameContaining(tableNames, toRemove);
  return tableNames;
}

std::vector<std::string> DatabaseMetadataGetter::getIndexNames() {
  //We just want indexes created by the user, their name are finishing by _IDX or by _I
  static cta::utils::Regex regexIndexes("(.*_IDX$)|(.*_I$)");
  auto indexNames = m_conn.getIndexNames();
  removeObjectNameNotMatches(indexNames, regexIndexes);
  return indexNames;
}

std::map<std::string, std::string, std::less<>> DatabaseMetadataGetter::getColumns(const std::string& tableName) {
  return m_conn.getColumns(tableName);
}

std::vector<std::string> DatabaseMetadataGetter::getConstraintNames(const std::string& tableName) {
  static constexpr std::array<std::string_view, 1> toRemove = {"CATALOGUE_STATUS_CONTENT_CK"};
  auto constraintNames = m_conn.getConstraintNames(tableName);
  //This constraint is added by ALTER TABLE, we can't check its existence for now
  removeObjectNameContaining(constraintNames, toRemove);
  return constraintNames;
}

std::vector<std::string> DatabaseMetadataGetter::getParallelTableNames() {
  return m_conn.getParallelTableNames();
}

std::vector<std::string> DatabaseMetadataGetter::getStoredProcedures() {
  return m_conn.getStoredProcedureNames();
}

std::vector<std::string> DatabaseMetadataGetter::getSynonyms() {
  return m_conn.getSynonymNames();
}

std::vector<std::string> DatabaseMetadataGetter::getTypes() {
  return m_conn.getTypeNames();
}

std::vector<std::string> DatabaseMetadataGetter::getErrorLoggingTables() {
  static cta::utils::Regex regex("(^ERR\\$_)");
  auto tableNames = DatabaseMetadataGetter::getTableNames();
  removeObjectNameNotMatches(tableNames, regex);
  return tableNames;
}

SQLiteDatabaseMetadataGetter::SQLiteDatabaseMetadataGetter(cta::rdbms::Conn& conn) : DatabaseMetadataGetter(conn) {}

std::vector<std::string> SQLiteDatabaseMetadataGetter::getIndexNames() {
  static constexpr std::array<std::string_view, 1> toRemove = {"sqlite_autoindex"};
  auto indexNames = DatabaseMetadataGetter::getIndexNames();
  //We do not want the sqlite_autoindex created automatically by SQLite
  removeObjectNameContaining(indexNames, toRemove);
  return indexNames;
}

std::vector<std::string> SQLiteDatabaseMetadataGetter::getTableNames() {
  static constexpr std::array<std::string_view, 1> toRemove = {"sqlite_sequence"};
  auto tableNames = DatabaseMetadataGetter::getTableNames();
  //We do not want the sqlite_sequence tables created automatically by SQLite
  removeObjectNameContaining(tableNames, toRemove);
  return tableNames;
}

cta::rdbms::Login::DbType SQLiteDatabaseMetadataGetter::getDbType() {
  return cta::rdbms::Login::DbType::DBTYPE_SQLITE;
}

std::set<std::string, std::less<>> SQLiteDatabaseMetadataGetter::getMissingIndexes() {
  // This check is for indexes on columns used by foreign key constraints. It is an optimisation for
  // production DBs. No need to check it for SQLite.
  return std::set<std::string, std::less<>>();
}

OracleDatabaseMetadataGetter::OracleDatabaseMetadataGetter(cta::rdbms::Conn& conn) : DatabaseMetadataGetter(conn) {}

cta::rdbms::Login::DbType OracleDatabaseMetadataGetter::getDbType() {
  return cta::rdbms::Login::DbType::DBTYPE_ORACLE;
}

std::vector<std::string> OracleDatabaseMetadataGetter::getTableNames() {
  static cta::utils::Regex regex("(^ERR\\$_)");
  auto tableNames = DatabaseMetadataGetter::getTableNames();
  //Ignore error logging tables
  removeObjectNameMatches(tableNames, regex);
  return tableNames;
}

std::set<std::string, std::less<>> OracleDatabaseMetadataGetter::getMissingIndexes() {
  // For definition of constraint types, see https://docs.oracle.com/en/database/oracle/oracle-database/12.2/refrn/USER_CONSTRAINTS.html
  const char* const sql = R"SQL(
    SELECT
      A.TABLE_NAME || '.' || A.COLUMN_NAME AS FQ_COL_NAME
    FROM
      USER_CONS_COLUMNS A,
      USER_CONSTRAINTS B
    WHERE
      A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND
      B.CONSTRAINT_TYPE = 'R' AND /* R = Referential Integrity */
      (A.TABLE_NAME || '.' || A.COLUMN_NAME) NOT IN
        (SELECT TABLE_NAME || '.' || COLUMN_NAME FROM USER_IND_COLUMNS)
  )SQL";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::set<std::string, std::less<>> columnNames;
  while (rset.next()) {
    columnNames.insert(rset.columnString("FQ_COL_NAME"));
  }
  return columnNames;
}

PostgresDatabaseMetadataGetter::PostgresDatabaseMetadataGetter(cta::rdbms::Conn& conn) : DatabaseMetadataGetter(conn) {}

cta::rdbms::Login::DbType PostgresDatabaseMetadataGetter::getDbType() {
  return cta::rdbms::Login::DbType::DBTYPE_POSTGRESQL;
}

std::set<std::string, std::less<>> PostgresDatabaseMetadataGetter::getMissingIndexes() {
  // Adapted from https://www.cybertec-postgresql.com/en/index-your-foreign-key/
  const char* const sql = R"SQL(
    SELECT
        T.RELNAME || '.' || A.ATTNAME AS FQ_COL_NAME
      FROM
        PG_CLASS T,
        PG_CATALOG.PG_CONSTRAINT C
      /* enumerated key column numbers per foreign key */
      CROSS JOIN LATERAL
        unnest(C.CONKEY) WITH ORDINALITY AS X(ATTNUM, n)
      /* name for each key column */
      JOIN
        PG_CATALOG.PG_ATTRIBUTE A ON A.ATTNUM = X.ATTNUM AND A.ATTRELID = C.CONRELID
      WHERE
        T.OID = C.CONRELID AND
        C.CONTYPE = 'f' AND
        NOT EXISTS (
          SELECT 1
          FROM PG_INDEX
          WHERE
            INDRELID = C.CONRELID AND
            (SELECT ARRAY(
              SELECT CONKEY[i]
              FROM generate_series(array_lower(CONKEY, 1), array_upper(CONKEY, 1)) i
              ORDER BY 1))
             =
            (SELECT ARRAY(
              SELECT INDKEY[i]
                FROM generate_series(array_lower(INDKEY, 1), array_upper(INDKEY, 1)) i
              ORDER BY 1))
        )
  )SQL";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::set<std::string, std::less<>> columnNames;
  while (rset.next()) {
    columnNames.insert(rset.columnString("FQ_COL_NAME"));
  }
  return columnNames;
}

DatabaseMetadataGetter* DatabaseMetadataGetterFactory::create(const rdbms::Login::DbType dbType,
                                                              cta::rdbms::Conn& conn) {
  using DbType = rdbms::Login::DbType;
  switch (dbType) {
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
      return std::make_unique<SQLiteDatabaseMetadataGetter>(conn).release();
    case DbType::DBTYPE_ORACLE:
      return std::make_unique<OracleDatabaseMetadataGetter>(conn).release();
    case DbType::DBTYPE_POSTGRESQL:
      return std::make_unique<PostgresDatabaseMetadataGetter>(conn).release();
    default:
      throw cta::exception::Exception(
        "In CatalogueMetadataGetterFactory::create(), can't get CatalogueMetadataGetter for dbType "
        + rdbms::Login::dbTypeToString(dbType));
  }
}

/**
 * SCHEMA METADATA GETTER methods
 */
SchemaMetadataGetter::SchemaMetadataGetter(std::unique_ptr<SQLiteDatabaseMetadataGetter> sqliteCatalogueMetadataGetter,
                                           const cta::rdbms::Login::DbType dbType)
    : m_sqliteDatabaseMetadataGetter(std::move(sqliteCatalogueMetadataGetter)),
      m_dbType(dbType) {}

std::vector<std::string> SchemaMetadataGetter::getIndexNames() {
  return m_sqliteDatabaseMetadataGetter->getIndexNames();
}

std::vector<std::string> SchemaMetadataGetter::getTableNames() {
  return m_sqliteDatabaseMetadataGetter->getTableNames();
}

std::map<std::string, std::string, std::less<>> SchemaMetadataGetter::getColumns(const std::string& tableName) {
  return m_sqliteDatabaseMetadataGetter->getColumns(tableName);
}

std::vector<std::string> SchemaMetadataGetter::getConstraintNames(const std::string& tableName) {
  static cta::utils::Regex regex("(^NN_)|(_NN$)");
  auto constraintNames = m_sqliteDatabaseMetadataGetter->getConstraintNames(tableName);
  if (m_dbType == cta::rdbms::Login::DbType::DBTYPE_POSTGRESQL) {
    //If the database to compare is POSTGRESQL, we cannot compare NOT NULL CONSTRAINT names
    //indeed, POSTGRESQL can not give the NOT NULL constraint names
    removeObjectNameMatches(constraintNames, regex);
  }
  return constraintNames;
}

}  // namespace cta::catalogue
