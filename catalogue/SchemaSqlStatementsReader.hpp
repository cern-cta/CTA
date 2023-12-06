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
#include "CatalogueSchema.hpp"

namespace cta::catalogue {

class SchemaSqlStatementsReader {
public:
  SchemaSqlStatementsReader() = default;
  explicit SchemaSqlStatementsReader(const cta::rdbms::Login::DbType dbType) : m_dbType(dbType) { }
  SchemaSqlStatementsReader(const SchemaSqlStatementsReader& orig) = default;
  virtual ~SchemaSqlStatementsReader() = default;
  virtual std::list<std::string> getStatements();

protected:
  cta::rdbms::Login::DbType m_dbType;
  /**
  * Separates the statements and put them in a std::list<std::string>
  * @param schema the sql statements put all together
  * @return a list containing separated statements from the schema passed in parameter
  */
  std::list<std::string> getAllStatementsFromSchema(const std::string &schema);
  /**
  * Returns the string corresponding to the database type
  * @return the string corresponding to the database type
  */
  std::string getDatabaseType();
};

/*
* This DirectoryVersionsSqlStatementsReader reads the sql statements from the files located in the allSchemasVersionPath directory
* This directory should contains all the directories containing the schema creation script specific to a schema VERSION
* Example of the content of the directory :
* catalogue
* |-- 1.0
* |  |-- oracle_catalogue_schema.sql
* |  |-- postgres_catalogue_schema.sql
* |  |....
* |-- 1.1
* |  |-- ...
* */
class DirectoryVersionsSqlStatementsReader : public SchemaSqlStatementsReader {
public:
  DirectoryVersionsSqlStatementsReader(const cta::rdbms::Login::DbType dbType, const std::string &catalogueVersion, const std::string &allSchemasVersionPath);
  DirectoryVersionsSqlStatementsReader(const DirectoryVersionsSqlStatementsReader& orig);
  virtual ~DirectoryVersionsSqlStatementsReader() = default;
  virtual std::list<std::string> getStatements();

private:
  std::string m_catalogueVersion;
  std::string m_allSchemasVersionPath;
  const std::string c_catalogueFileNameTrailer = "_catalogue_schema.sql";
  /**
  * Return the schema located in SCHEMA_VERSION/dbType_catalogue_schema.sql
  * @return the string containing the sql statements for the creation of the schema
  */
  std::string readSchemaFromFile();
  std::string getSchemaFilePath();
};

class MapSqlStatementsReader : public SchemaSqlStatementsReader {
public:
  MapSqlStatementsReader(const cta::rdbms::Login::DbType dbType, const std::string &catalogueVersion) :
    SchemaSqlStatementsReader(dbType), m_catalogueVersion(catalogueVersion) { }
  MapSqlStatementsReader(const MapSqlStatementsReader& orig) = default;
  virtual ~MapSqlStatementsReader() = default;
  virtual std::list<std::string> getStatements();
private:
  std::string m_catalogueVersion;
};

class CppSchemaStatementsReader: public SchemaSqlStatementsReader{
public:
  explicit CppSchemaStatementsReader(const cta::catalogue::CatalogueSchema& schema);
  std::list<std::string> getStatements();
private:
  const cta::catalogue::CatalogueSchema m_schema;
};

} // namespace cta::catalogue
