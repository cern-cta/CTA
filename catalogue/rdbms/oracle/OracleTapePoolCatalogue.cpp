/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/oracle/OracleTapePoolCatalogue.hpp"

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

OracleTapePoolCatalogue::OracleTapePoolCatalogue(log::Logger& log,
                                                 std::shared_ptr<rdbms::ConnPool> connPool,
                                                 RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsTapePoolCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t OracleTapePoolCatalogue::getNextTapePoolId(rdbms::Conn& conn) const {
  const char* const sql = R"SQL(
    SELECT
      TAPE_POOL_ID_SEQ.NEXTVAL AS TAPE_POOL_ID
    FROM
      DUAL
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception(std::string("Result set is unexpectedly empty"));
  }

  return rset.columnUint64("TAPE_POOL_ID");
}

}  // namespace cta::catalogue
