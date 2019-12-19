/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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
#include "rdbms/Login.hpp"
#include "rdbms/Conn.hpp"
#include "SchemaComparer.hpp"

namespace cta{
namespace catalogue{
    
class SchemaChecker {
public:
  enum Status {
    OK,
    FAILURE
  };
  SchemaChecker(rdbms::Login::DbType dbType,cta::rdbms::Conn &conn);
  virtual ~SchemaChecker();
  void useSQLiteSchemaComparer(const std::string &allSchemasDirectoryPath);
  Status compareSchema();
  void checkNoParallelTables();
private:
  cta::rdbms::Login::DbType m_dbType;
  cta::rdbms::Conn &m_catalogueConn;
  std::unique_ptr<SchemaComparer> m_schemaComparer;
};

}}


