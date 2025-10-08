/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/oracle/OracleStorageClassCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

OracleStorageClassCatalogue::OracleStorageClassCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsStorageClassCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t OracleStorageClassCatalogue::getNextStorageClassId(rdbms::Conn &conn) {
  const char* const sql = R"SQL(
    SELECT
      STORAGE_CLASS_ID_SEQ.NEXTVAL AS STORAGE_CLASS_ID
    FROM
      DUAL
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception(std::string("Result set is unexpectedly empty"));
  }

  return rset.columnUint64("STORAGE_CLASS_ID");
}

} // namespace cta::catalogue
