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

#include "catalogue/SqliteCatalogue.hpp"
#include "catalogue/RdbmsCatalogueSchema.hpp"
#include "catalogue/SqliteConn.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue(const std::string &filename) {
  std::unique_ptr<SqliteConn> sqliteConn(new SqliteConn(filename));
  m_conn.reset(sqliteConn.release());
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SqliteCatalogue::SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
SqliteCatalogue::~SqliteCatalogue() {
}

//------------------------------------------------------------------------------
// getNextArchiveFileId
//------------------------------------------------------------------------------
uint64_t SqliteCatalogue::getNextArchiveFileId() {
  try {
    m_conn->executeNonQuery("BEGIN EXCLUSIVE;");
    m_conn->executeNonQuery("UPDATE ARCHIVE_FILE_ID SET ID = ID + 1;");
    uint64_t archiveFileId = 0;
    {
      const char *const sql =
        "SELECT "
          "ID AS ID "
        "FROM "
          "ARCHIVE_FILE_ID";
      std::unique_ptr<DbStmt> stmt(m_conn->createStmt(sql));
      std::unique_ptr<DbRset> rset(stmt->executeQuery());
      if(!rset->next()) {
        throw exception::Exception("ARCHIVE_FILE_ID table is empty");
      }
      archiveFileId = rset->columnUint64("ID");
      if(rset->next()) {
        throw exception::Exception("Found more than one ID counter in the ARCHIVE_FILE_ID table");
      }
    }
    m_conn->commit();

    return archiveFileId;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
