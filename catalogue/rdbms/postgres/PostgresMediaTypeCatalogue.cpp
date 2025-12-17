/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresMediaTypeCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresMediaTypeCatalogue::PostgresMediaTypeCatalogue(log::Logger& log,
                                                       std::shared_ptr<rdbms::ConnPool> connPool,
                                                       RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsMediaTypeCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresMediaTypeCatalogue::getNextMediaTypeId(rdbms::Conn& conn) const {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('MEDIA_TYPE_ID_SEQ') AS MEDIA_TYPE_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("MEDIA_TYPE_ID");
}

}  // namespace cta::catalogue
