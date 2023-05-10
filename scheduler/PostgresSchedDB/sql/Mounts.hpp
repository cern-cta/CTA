/**
 * @project        The CERN Tape Archive (CTA)
 * @description    
 * @copyright      Copyright © 2021-2022 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

namespace cta {
namespace postgresscheddb {
namespace sql {

struct MountsRow {
  uint64_t mountId;
  time_t creationTime;
  std::string owner;

  MountsRow() :
    mountId(0),
    creationTime(0) { }

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  MountsRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  MountsRow& operator=(const rdbms::Rset &rset) {
    mountId      = rset.columnUint64("MOUNT_ID");
    creationTime = rset.columnUint64("CREATION_TIMESTAMP");
    owner        = rset.columnString("OWNER");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer& params) const {
    params.add("mountId", mountId);
    params.add("creationTime", creationTime);
    params.add("owner", owner);
  }

  /**
   * Increment Mount Id and return the new value
   *
   * @return result set containing the one row in the table
   */
  static rdbms::Rset insertMountAndSelect(Transaction &txn, const std::string& owner) {
    const char *const sql = "INSERT INTO TAPE_MOUNTS ("
      "OWNER) VALUES ("
      ":OWNER"
      ") RETURNING "
      "MOUNT_ID,"
      "EXTRACT(EPOCH FROM CREATION_TIME AT TIME ZONE 'UTC')::BIGINT AS CREATION_TIMESTAMP,"
      "OWNER";

    auto stmt = txn.conn().createStmt(sql);
    stmt.bindString(":OWNER", owner);

    return stmt.executeQuery();
  }
};

} // namespace sql
} // namespace postgresscheddb
} // namespace cta
