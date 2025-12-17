/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/rdbms/postgres/PostgresPhysicalLibraryCatalogue.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresPhysicalLibraryCatalogue::PostgresPhysicalLibraryCatalogue(log::Logger& log,
                                                                   std::shared_ptr<rdbms::ConnPool> connPool,
                                                                   RdbmsCatalogue* rdbmsCatalogue)
    : RdbmsPhysicalLibraryCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresPhysicalLibraryCatalogue::getNextPhysicalLibraryId(rdbms::Conn& conn) const {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('PHYSICAL_LIBRARY_ID_SEQ') AS PHYSICAL_LIBRARY_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if (!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("PHYSICAL_LIBRARY_ID");
}

}  // namespace cta::catalogue
