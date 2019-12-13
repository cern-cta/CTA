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

#include "SchemaComparer.hpp"

namespace cta {
namespace catalogue {
  
class SQLiteSchemaComparer: public SchemaComparer {
public:
  SQLiteSchemaComparer(const cta::rdbms::Login::DbType &catalogueDbType, rdbms::Conn &catalogueConnPool);
  SchemaComparerResult compare() override;
  virtual ~SQLiteSchemaComparer();
  
private:
  void insertSchemaInSQLite();
  SchemaComparerResult compareIndexes();
  SchemaComparerResult compareTables() override;
  rdbms::Conn m_sqliteConn;
  std::unique_ptr<SQLiteCatalogueMetadataGetter> m_sqliteSchemaMetadataGetter;  
};

}}