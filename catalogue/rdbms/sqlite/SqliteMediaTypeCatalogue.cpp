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

#include "catalogue/rdbms/sqlite/SqliteMediaTypeCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

SqliteMediaTypeCatalogue::SqliteMediaTypeCatalogue(log::Logger &log,
  std::shared_ptr<rdbms::ConnPool> connPool, RdbmsCatalogue* rdbmsCatalogue)
  : RdbmsMediaTypeCatalogue(log, connPool, rdbmsCatalogue) {}

uint64_t SqliteMediaTypeCatalogue::getNextMediaTypeId(rdbms::Conn &conn) const {
  try {
    conn.executeNonQuery("INSERT INTO MEDIA_TYPE_ID VALUES(NULL)");
    uint64_t id = 0;
    {
      const char *const sql = "SELECT LAST_INSERT_ROWID() AS ID";
      auto stmt = conn.createStmt(sql);
      auto rset = stmt.executeQuery();
      if(!rset.next()) {
        throw exception::Exception(std::string("Unexpected empty result set for '") + sql + "\'");
      }
      id = rset.columnUint64("ID");
      if(rset.next()) {
        throw exception::Exception(std::string("Unexpectedly found more than one row in the result of '") + sql + "\'");
      }
    }
    conn.executeNonQuery("DELETE FROM MEDIA_TYPE_ID");

    return id;
  } catch(exception::UserError &) {
    throw;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
    throw;
  }
}

}  // namespace catalogue
}  // namespace cta