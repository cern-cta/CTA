/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "catalogue/rdbms/postgres/PostgresTapePoolCatalogue.hpp"
#include "catalogue/rdbms/RdbmsCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

PostgresTapePoolCatalogue::PostgresTapePoolCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsTapePoolCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t PostgresTapePoolCatalogue::getNextTapePoolId(rdbms::Conn &conn) const {
  const char* const sql = R"SQL(
    SELECT NEXTVAL('TAPE_POOL_ID_SEQ') AS TAPE_POOL_ID
  )SQL";
  auto stmt = conn.createStmt(sql);
  auto rset = stmt.executeQuery();
  if(!rset.next()) {
    throw exception::Exception("Result set is unexpectedly empty");
  }
  return rset.columnUint64("TAPE_POOL_ID");
}

} // namespace cta::catalogue
