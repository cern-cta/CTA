/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresTapeCatalogue.hpp"

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresTapeCatalogue::PostgresTapeCatalogue(log::Logger& log,
                                             std::shared_ptr<rdbms::ConnPool> connPool,
                                             RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsTapeCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresTapeCatalogue::getTapeLastFSeq(rdbms::Conn& conn, const std::string& vid) const {
  const char* const sql = R"SQL(
    SELECT
      LAST_FSEQ AS LAST_FSEQ
    FROM
      TAPE
    WHERE
      VID = :VID
  )SQL";
  auto stmt = conn.createStmt(sql);
  stmt.bindString(":VID", vid);
  auto rset = stmt.executeQuery();
  if (rset.next()) {
    return rset.columnUint64("LAST_FSEQ");
  } else {
    throw exception::Exception(std::string("No such tape with vid=") + vid);
  }
}

}  // namespace cta::catalogue
