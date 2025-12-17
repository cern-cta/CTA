/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresStorageClassCatalogue.hpp"

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresStorageClassCatalogue::PostgresStorageClassCatalogue(log::Logger& log,
                                                             std::shared_ptr<rdbms::ConnPool> connPool,
                                                             RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsStorageClassCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresStorageClassCatalogue::getNextStorageClassId(rdbms::Conn& conn) {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('STORAGE_CLASS_ID_SEQ') AS STORAGE_CLASS_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("STORAGE_CLASS_ID");
}

}  // namespace cta::catalogue
