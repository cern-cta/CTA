/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/sqlite/SqliteTapeCatalogue.hpp"

#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

SqliteTapeCatalogue::SqliteTapeCatalogue(log::Logger& log,
                                         std::shared_ptr<rdbms::ConnPool> connPool,
                                         RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsTapeCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t SqliteTapeCatalogue::getTapeLastFSeq(rdbms::Conn& conn, const std::string& vid) const {
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
  if (!rset.next()) {
    throw exception::Exception(std::string("The tape with VID " + vid + " does not exist"));
  }

  return rset.columnUint64("LAST_FSEQ");
}
}  // namespace cta::catalogue
