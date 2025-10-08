/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/oracle/OraclePhysicalLibraryCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

OraclePhysicalLibraryCatalogue::OraclePhysicalLibraryCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsPhysicalLibraryCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t OraclePhysicalLibraryCatalogue::getNextPhysicalLibraryId(rdbms::Conn &conn) const {
  const char* const sql = R"SQL(
    SELECT
      PHYSICAL_LIBRARY_ID_SEQ.NEXTVAL AS PHYSICAL_LIBRARY_ID
    FROM
      DUAL
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception(std::string("Result set is unexpectedly empty"));
  }

  return rset.columnUint64("PHYSICAL_LIBRARY_ID");
}

} // namespace cta::catalogue
