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

#include "DatabaseMetadataGetter.hpp"
#include <algorithm>

namespace cta {
namespace catalogue {


MetadataGetter::~MetadataGetter(){}

void MetadataGetter::removeObjectNameContaining(std::list<std::string>& objects, const std::list<std::string> &wordsToTriggerRemoval){
  objects.remove_if([&wordsToTriggerRemoval](const std::string &object){
    return std::find_if(wordsToTriggerRemoval.begin(), wordsToTriggerRemoval.end(),[&object](const std::string &wordTriggeringRemoval){
      return object.find(wordTriggeringRemoval) != std::string::npos;
    }) != wordsToTriggerRemoval.end();
  });
}

void MetadataGetter::removeObjectNameNotContaining(std::list<std::string>& objects, const std::list<std::string> &wordsNotToTriggerRemoval){
  objects.remove_if([&wordsNotToTriggerRemoval](const std::string &object){
    return std::find_if(wordsNotToTriggerRemoval.begin(), wordsNotToTriggerRemoval.end(),[&object](const std::string &wordsNotToTriggeringRemoval){
      return object.find(wordsNotToTriggeringRemoval) == std::string::npos;
    }) != wordsNotToTriggerRemoval.end();
  });
}

void MetadataGetter::removeObjectNameNotMatches(std::list<std::string> &objects, const cta::utils::Regex &regex){
  objects.remove_if([&regex](const std::string &object){
    return !regex.has_match(object);
  });
}

void MetadataGetter::removeObjectNameMatches(std::list<std::string> &objects, const cta::utils::Regex &regex){
  objects.remove_if([&regex](const std::string &object){
    return regex.has_match(object);
  });
}

DatabaseMetadataGetter::DatabaseMetadataGetter(cta::rdbms::Conn& conn):m_conn(conn){}

SchemaVersion DatabaseMetadataGetter::getCatalogueVersion(){
  const char *const sql =
    "SELECT "
      "CTA_CATALOGUE.SCHEMA_VERSION_MAJOR AS SCHEMA_VERSION_MAJOR,"
      "CTA_CATALOGUE.SCHEMA_VERSION_MINOR AS SCHEMA_VERSION_MINOR "
    "FROM "
      "CTA_CATALOGUE";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  if(rset.next()) {
    SchemaVersion::Builder schemaVersionBuilder;
    schemaVersionBuilder.schemaVersionMajor(rset.columnUint64("SCHEMA_VERSION_MAJOR"))
                        .schemaVersionMinor(rset.columnUint64("SCHEMA_VERSION_MINOR"))
                        //By default, the status is set as PRODUCTION (to be backward-compatible with version 1.0 of the schema)
                        .status(SchemaVersion::Status::PRODUCTION);

    //The cta-catalogue-schema-verify tool has to be backward-compatible with version 1.0
    //of the schema that does not have the NEXT_SCHEMA_VERSION_MAJOR, NEXT_SCHEMA_VERSION_MINOR and the STATUS column
    const char *const sql2 =
    "SELECT "
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MAJOR AS NEXT_SCHEMA_VERSION_MAJOR,"
        "CTA_CATALOGUE.NEXT_SCHEMA_VERSION_MINOR AS NEXT_SCHEMA_VERSION_MINOR,"
        "CTA_CATALOGUE.STATUS AS STATUS "
      "FROM "
        "CTA_CATALOGUE";

    auto stmt2 = m_conn.createStmt(sql2);
    try{
      auto rset2 = stmt2.executeQuery();
      if(rset2.next()){
        auto schemaVersionMajorNext = rset2.columnOptionalUint64("NEXT_SCHEMA_VERSION_MAJOR");
        auto schemaVersionMinorNext = rset2.columnOptionalUint64("NEXT_SCHEMA_VERSION_MINOR");
        auto schemaStatus = rset2.columnString("STATUS");
        if(schemaVersionMajorNext && schemaVersionMinorNext){
          schemaVersionBuilder.nextSchemaVersionMajor(schemaVersionMajorNext.value())
                              .nextSchemaVersionMinor(schemaVersionMinorNext.value())
                              .status(schemaStatus);
        }
      }
    } catch (const cta::exception::Exception &ex){
    }
    return schemaVersionBuilder.build();
  } else {
    throw exception::Exception("CTA_CATALOGUE does not contain any row");
  }
}

std::list<std::string> DatabaseMetadataGetter::getTableNames(){
  std::list<std::string> tableNames = m_conn.getTableNames();
  removeObjectNameContaining(tableNames,{"DATABASECHANGELOG","DATABASECHANGELOGLOCK"});
  return tableNames;
}

std::list<std::string> DatabaseMetadataGetter::getIndexNames(){
  std::list<std::string> indexNames = m_conn.getIndexNames();
  //We just want indexes created by the user, their name are finishing by _IDX or by _I
  cta::utils::Regex regexIndexes("(.*_IDX$)|(.*_I$)");
  removeObjectNameNotMatches(indexNames,regexIndexes);
  return indexNames;
}

std::map<std::string,std::string> DatabaseMetadataGetter::getColumns(const std::string& tableName){
  return m_conn.getColumns(tableName);
}

std::list<std::string> DatabaseMetadataGetter::getConstraintNames(const std::string &tableName){
  std::list<std::string> constraintNames = m_conn.getConstraintNames(tableName);
  //This constraint is added by ALTER TABLE, we can't check its existence for now
  removeObjectNameContaining(constraintNames,{"CATALOGUE_STATUS_CONTENT_CK"});
  return constraintNames;
}

std::list<std::string> DatabaseMetadataGetter::getParallelTableNames(){
  return m_conn.getParallelTableNames();
}

std::list<std::string> DatabaseMetadataGetter::getStoredProcedures() {
  return m_conn.getStoredProcedureNames();
}

std::list<std::string> DatabaseMetadataGetter::getSynonyms(){
  return m_conn.getSynonymNames();
}

std::list<std::string> DatabaseMetadataGetter::getTypes() {
  return m_conn.getTypeNames();
}

std::list<std::string> DatabaseMetadataGetter::getErrorLoggingTables() {
  std::list<std::string> tableNames = DatabaseMetadataGetter::getTableNames();
  cta::utils::Regex regex("(^ERR\\$_)");
  removeObjectNameNotMatches(tableNames,regex);
  return tableNames;
}

DatabaseMetadataGetter::~DatabaseMetadataGetter() {}

SQLiteDatabaseMetadataGetter::SQLiteDatabaseMetadataGetter(cta::rdbms::Conn & conn):DatabaseMetadataGetter(conn){}
SQLiteDatabaseMetadataGetter::~SQLiteDatabaseMetadataGetter(){}

std::list<std::string> SQLiteDatabaseMetadataGetter::getIndexNames() {
  std::list<std::string> indexNames = DatabaseMetadataGetter::getIndexNames();
  //We do not want the sqlite_autoindex created automatically by SQLite
  removeObjectNameContaining(indexNames,{"sqlite_autoindex"});
  return indexNames;
}

std::list<std::string> SQLiteDatabaseMetadataGetter::getTableNames(){
  std::list<std::string> tableNames = DatabaseMetadataGetter::getTableNames();
  //We do not want the sqlite_sequence tables created automatically by SQLite
  removeObjectNameContaining(tableNames,{"sqlite_sequence"});
  return tableNames;
}

cta::rdbms::Login::DbType SQLiteDatabaseMetadataGetter::getDbType(){
  return cta::rdbms::Login::DbType::DBTYPE_SQLITE;
}

std::set<std::string> SQLiteDatabaseMetadataGetter::getMissingIndexes() {
  // This check is for indexes on columns used by foreign key constraints. It is an optimisation for
  // production DBs. No need to check it for SQLite.
  return std::set<std::string>();
}

OracleDatabaseMetadataGetter::OracleDatabaseMetadataGetter(cta::rdbms::Conn & conn):DatabaseMetadataGetter(conn){}
OracleDatabaseMetadataGetter::~OracleDatabaseMetadataGetter(){}

cta::rdbms::Login::DbType OracleDatabaseMetadataGetter::getDbType(){
  return cta::rdbms::Login::DbType::DBTYPE_ORACLE;
}

std::list<std::string> OracleDatabaseMetadataGetter::getTableNames(){
  std::list<std::string> tableNames = DatabaseMetadataGetter::getTableNames();
  //Ignore error logging tables
  removeObjectNameMatches(tableNames,cta::utils::Regex("(^ERR\\$_)"));
  return tableNames;
}

std::set<std::string> OracleDatabaseMetadataGetter::getMissingIndexes() {
  // For definition of constraint types, see https://docs.oracle.com/en/database/oracle/oracle-database/12.2/refrn/USER_CONSTRAINTS.html
  const char* const sql = "SELECT "
      "A.TABLE_NAME || '.' || A.COLUMN_NAME AS FQ_COL_NAME "
    "FROM "
      "USER_CONS_COLUMNS A,"
      "USER_CONSTRAINTS B "
    "WHERE "
      "A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND "
      "B.CONSTRAINT_TYPE = 'R' AND " // R = Referential Integrity
      "(A.TABLE_NAME || '.' || A.COLUMN_NAME) NOT IN"
        "(SELECT TABLE_NAME || '.' || COLUMN_NAME FROM USER_IND_COLUMNS)";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::set<std::string> columnNames;
  while(rset.next()) {
    columnNames.insert(rset.columnString("FQ_COL_NAME"));
  }
  return columnNames;
}

PostgresDatabaseMetadataGetter::PostgresDatabaseMetadataGetter(cta::rdbms::Conn& conn):DatabaseMetadataGetter(conn) {}
PostgresDatabaseMetadataGetter::~PostgresDatabaseMetadataGetter(){}
cta::rdbms::Login::DbType PostgresDatabaseMetadataGetter::getDbType(){
  return cta::rdbms::Login::DbType::DBTYPE_POSTGRESQL;
}

std::set<std::string> PostgresDatabaseMetadataGetter::getMissingIndexes() {
  // Adapted from https://www.cybertec-postgresql.com/en/index-your-foreign-key/
  const char* const sql = "SELECT "
        "T.RELNAME || '.' || A.ATTNAME AS FQ_COL_NAME "
      "FROM "
        "PG_CLASS T, "
        "PG_CATALOG.PG_CONSTRAINT C "
      // enumerated key column numbers per foreign key
      "CROSS JOIN LATERAL "
        "unnest(C.CONKEY) WITH ORDINALITY AS X(ATTNUM, n) "
      // name for each key column
      "JOIN "
        "PG_CATALOG.PG_ATTRIBUTE A ON A.ATTNUM = X.ATTNUM AND A.ATTRELID = C.CONRELID "
      "WHERE "
        "T.OID = C.CONRELID AND "
        "C.CONTYPE = 'f' AND "
        "NOT EXISTS ("
          "SELECT 1 "
          "FROM PG_INDEX "
          "WHERE "
            "INDRELID = C.CONRELID AND "
            "(SELECT ARRAY( "
              "SELECT CONKEY[i] "
              "FROM generate_series(array_lower(CONKEY, 1), array_upper(CONKEY, 1)) i "
              "ORDER BY 1))"
            " = "
            "(SELECT ARRAY( "
              "SELECT INDKEY[i] "
                "FROM generate_series(array_lower(INDKEY, 1), array_upper(INDKEY, 1)) i "
              "ORDER BY 1)) "
        ")";

  auto stmt = m_conn.createStmt(sql);
  auto rset = stmt.executeQuery();

  std::set<std::string> columnNames;
  while(rset.next()) {
    columnNames.insert(rset.columnString("FQ_COL_NAME"));
  }
  return columnNames;
}

DatabaseMetadataGetter * DatabaseMetadataGetterFactory::create(const rdbms::Login::DbType dbType, cta::rdbms::Conn & conn) {
  typedef rdbms::Login::DbType DbType;
  switch(dbType){
    case DbType::DBTYPE_IN_MEMORY:
    case DbType::DBTYPE_SQLITE:
      return new SQLiteDatabaseMetadataGetter(conn);
    case DbType::DBTYPE_ORACLE:
      return new OracleDatabaseMetadataGetter(conn);
    case DbType::DBTYPE_POSTGRESQL:
      return new PostgresDatabaseMetadataGetter(conn);
    default:
      throw cta::exception::Exception("In CatalogueMetadataGetterFactory::create(), can't get CatalogueMetadataGetter for dbType "+rdbms::Login::dbTypeToString(dbType));
  }
}

/**
 * SCHEMA METADATA GETTER methods
 */
SchemaMetadataGetter::SchemaMetadataGetter(std::unique_ptr<SQLiteDatabaseMetadataGetter> sqliteCatalogueMetadataGetter, const cta::rdbms::Login::DbType dbType):m_sqliteDatabaseMetadataGetter(std::move(sqliteCatalogueMetadataGetter)),m_dbType(dbType) {}

std::list<std::string> SchemaMetadataGetter::getIndexNames() {
  return m_sqliteDatabaseMetadataGetter->getIndexNames();
}

std::list<std::string> SchemaMetadataGetter::getTableNames() {
  return m_sqliteDatabaseMetadataGetter->getTableNames();
}

std::map<std::string,std::string> SchemaMetadataGetter::getColumns(const std::string& tableName) {
  return m_sqliteDatabaseMetadataGetter->getColumns(tableName);
}

std::list<std::string> SchemaMetadataGetter::getConstraintNames(const std::string& tableName) {
  std::list<std::string> constraintNames = m_sqliteDatabaseMetadataGetter->getConstraintNames(tableName);
  if(m_dbType == cta::rdbms::Login::DbType::DBTYPE_POSTGRESQL){
    //If the database to compare is POSTGRESQL, we cannot compare NOT NULL CONSTRAINT names
    //indeed, POSTGRESQL can not give the NOT NULL constraint names
    removeObjectNameMatches(constraintNames,cta::utils::Regex("(^NN_)|(_NN$)"));
  }
  return constraintNames;
}

}}
