/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/SchemaCreatingSqliteCatalogue.hpp"
#include "catalogue/RdbmsCatalogueSchema.hpp"
#include "rdbms/SqliteConn.hpp"
#include "rdbms/SqliteConnFactory.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SchemaCreatingSqliteCatalogue::SchemaCreatingSqliteCatalogue(const std::string &filename, const uint64_t nbConns):
  SqliteCatalogue(filename, nbConns) {
  createCatalogueSchema();
}

//------------------------------------------------------------------------------
// createCatalogueSchema
//------------------------------------------------------------------------------
void SchemaCreatingSqliteCatalogue::createCatalogueSchema() {
  const RdbmsCatalogueSchema schema;
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;
  auto conn = m_connPool.getConn();

  while(std::string::npos != (findResult = schema.sql.find(';', searchPos))) {
    const std::string::size_type length = findResult - searchPos + 1;
    const std::string sql = schema.sql.substr(searchPos, length);
    searchPos = findResult + 1;
    auto stmt = conn->createStmt(sql, rdbms::Stmt::AutocommitMode::ON);
    stmt->executeNonQuery();
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SchemaCreatingSqliteCatalogue::~SchemaCreatingSqliteCatalogue() {
}

} // namespace catalogue
} // namespace cta
