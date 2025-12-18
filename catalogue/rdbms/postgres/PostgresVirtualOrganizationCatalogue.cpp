/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresVirtualOrganizationCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresVirtualOrganizationCatalogue::PostgresVirtualOrganizationCatalogue(log::Logger& log,
                                                                           std::shared_ptr<rdbms::ConnPool> connPool,
                                                                           RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsVirtualOrganizationCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresVirtualOrganizationCatalogue::getNextVirtualOrganizationId(rdbms::Conn& conn) {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('VIRTUAL_ORGANIZATION_ID_SEQ') AS VIRTUAL_ORGANIZATION_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("VIRTUAL_ORGANIZATION_ID");
}

}  // namespace cta::catalogue
