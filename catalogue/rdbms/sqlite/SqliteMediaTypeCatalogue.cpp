/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/sqlite/SqliteMediaTypeCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

SqliteMediaTypeCatalogue::SqliteMediaTypeCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsMediaTypeCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t SqliteMediaTypeCatalogue::getNextMediaTypeId(rdbms::Conn &conn) const {
  conn.executeNonQuery(R"SQL(INSERT INTO MEDIA_TYPE_ID VALUES(NULL))SQL");
  uint64_t id = 0;
  const char* const sql = R"SQL(
    SELECT LAST_INSERT_ROWID() AS ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
  }
  id = rset.columnUint64("ID");
  if(rset.next()) {
    throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
  }
  conn.executeNonQuery(R"SQL(DELETE FROM MEDIA_TYPE_ID)SQL");

  return id;
}

} // namespace cta::catalogue
