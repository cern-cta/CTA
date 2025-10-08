/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/sqlite/SqlitePhysicalLibraryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

SqlitePhysicalLibraryCatalogue::SqlitePhysicalLibraryCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsPhysicalLibraryCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t SqlitePhysicalLibraryCatalogue::getNextPhysicalLibraryId(rdbms::Conn &conn) const {
  conn.executeNonQuery(R"SQL(INSERT INTO PHYSICAL_LIBRARY_ID VALUES(NULL))SQL");
  uint64_t physicalLibraryId = 0;
  const char* const sql = R"SQL(
    SELECT LAST_INSERT_ROWID() AS ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
  }
  physicalLibraryId = rset.columnUint64("ID");
  if(rset.next()) {
    throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
  }
  conn.executeNonQuery(R"SQL(DELETE FROM PHYSICAL_LIBRARY_ID)SQL");

  return physicalLibraryId;
}

} // namespace cta::catalogue
