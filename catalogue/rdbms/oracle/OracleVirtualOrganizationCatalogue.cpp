/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <string>

#include "catalogue/rdbms/oracle/OracleVirtualOrganizationCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

OracleVirtualOrganizationCatalogue::OracleVirtualOrganizationCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsVirtualOrganizationCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t OracleVirtualOrganizationCatalogue::getNextVirtualOrganizationId(rdbms::Conn &conn) {
  const char* const sql = R"SQL(
    SELECT
      VIRTUAL_ORGANIZATION_ID_SEQ.NEXTVAL AS VIRTUAL_ORGANIZATION_ID
    FROM
      DUAL
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception(std::string("Result set is unexpectedly empty"));
  }

  return rset.columnUint64("VIRTUAL_ORGANIZATION_ID");
}

} // namespace cta::catalogue
