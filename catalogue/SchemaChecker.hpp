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

#pragma once
#include "rdbms/Login.hpp"
#include "rdbms/Conn.hpp"
#include "SchemaComparer.hpp"
#include "CatalogueSchema.hpp"
#include "SchemaCheckerResult.hpp"

namespace cta::catalogue {

/**
 * This class allows to check the catalogue schema validity running in a specific database
 * The database is identified thanks to its connection
 *
 * The checking of the database consists of
 * 1. comparing the schema running in the database with the one
 * defined in the catalogue/schema_version directory
 * 2. checking if the database contains tables that have been set as PARALLEL
 *
 * The comparing part of the checking is done by a SchemaComparer class that will tell the differences between
 * the schema defined by the user and the one in the catalogue database.
 */
class SchemaChecker {
public:
  /**
   * Constructor of the SchemaChecker class
   * @param dbType the type of the database to check against
   * @param conn the connection of the database to check
   */
  SchemaChecker(const std::string& databaseToCheckName, rdbms::Login::DbType dbType, cta::rdbms::Conn &conn);

  /**
   * Destructor
   */
  virtual ~SchemaChecker() = default;

  /**
   * Compare the schema by using a SchemaComparer and output what it is doing and errors
   * @throws Exception if no SchemaComparer has been set.
   * @return a SchemaCheckerResult
   */
  SchemaCheckerResult displayingCompareSchema(std::ostream & stdout, std::ostream & stderr);

  /**
   * Checks if the catalogue database contains PARALLEL tables
   * It will display a warning with the table name if this is the case
   * @return a SchemaCheckerResult containing the warnings
   */
  SchemaCheckerResult warnParallelTables();

  /**
   * Checks if the catalogue database schema is upgrading or not
   * @return a SchemaCheckerResult containing the warnings
   */
  SchemaCheckerResult warnSchemaUpgrading();

  /**
   * Compare the schema tables whose names are located in the tableList parameter
   * @param tableList the table names we would like to compare
   * @return a SchemaCheckerResult containing the errors
   */
  SchemaCheckerResult compareTablesLocatedInSchema();

  /**
   * Checks if the table tableName contains the columns whose names are in the columnNames list
   * @param tableName the tableName to check
   * @param columnNames the columnName the tableName is supposed to have
   * @return a SchemaCheckerResult containing the errors
   */
  SchemaCheckerResult checkTableContainsColumns(const std::string &tableName, const std::list<std::string>& columnNames);

  /**
   * Checks if there are stored procedures in the database.
   * @return a SchemaCheckerResult containing warnings if there are stored procedures.
   */
  SchemaCheckerResult warnProcedures();

  /**
   * Checks if there are synonyms in the database.
   * @return a SchemaCheckerResult containing warnings if there are synonyms
   */
  SchemaCheckerResult warnSynonyms();

  /**
   * Checks if there are types in the database
   * @return a SchemaCheckerResult containing warnings if there are types in the database
   */
  SchemaCheckerResult warnTypes();

  /**
   * Checks if there are error logging tables in the database
   * @return a SchemaCheckerResult containing warnings if there are error logging tables in the database
   */
  SchemaCheckerResult warnErrorLoggingTables();

  /**
   * Checks that all foreign key constraints have an index on both sides of the constraint
   * @return a SchemaCheckerResult containing warnings if there are missing indexes
   */
  SchemaCheckerResult warnMissingIndexes();

  class Builder {
  public:
    Builder(const std::string& databaseToCheckName, const cta::rdbms::Login::DbType& dbType, cta::rdbms::Conn &conn);
    Builder & useSQLiteSchemaComparer();
    Builder & useDirectorySchemaReader(const std::string &allSchemasVersionsDirectory);
    Builder & useMapStatementsReader();
    Builder & useCppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema);
    std::unique_ptr<SchemaChecker> build();
  private:
    const std::string m_databaseToCheckName;
    cta::rdbms::Login::DbType m_dbType;
    cta::rdbms::Conn &m_catalogueConn;
    std::unique_ptr<SchemaComparer> m_schemaComparer;
    std::unique_ptr<DatabaseMetadataGetter> m_databaseMetadataGetter;
    std::unique_ptr<SchemaSqlStatementsReader> m_schemaSqlStatementsReader;
  };

private:
  const std::string m_databaseToCheckName;
  /**
   * Catalogue-to-check database type
   */
  cta::rdbms::Login::DbType m_dbType;
  /**
   * Catalogue-to-check database connection
   */
  cta::rdbms::Conn &m_catalogueConn;
  /**
   * SchemaComparer class to compare the database schema
   */
  std::unique_ptr<SchemaComparer> m_schemaComparer;

  /**
   * Catalogue metadata getter that allows to query the
   * metadatas of the catalogue database
   */
  std::unique_ptr<DatabaseMetadataGetter> m_databaseMetadataGetter;

  void checkSchemaComparerNotNull(const std::string & methodName);
};

} // namespace cta::catalogue
