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

namespace cta::postgresscheddb::sql {

struct MountsRow {
  uint64_t mountId = 0;
  time_t creationTime = 0;
  std::string owner;

  MountsRow() = default;

  /**
   * Constructor from row
   *
   * @param row  A single row from the result of a query
   */
  explicit MountsRow(const rdbms::Rset &rset) {
    *this = rset;
  }

  MountsRow &operator=(const rdbms::Rset &rset) {
    mountId = rset.columnUint64("MOUNT_ID");
    creationTime = rset.columnUint64("CREATION_TIMESTAMP");
    owner = rset.columnString("OWNER");
    return *this;
  }

  void addParamsToLogContext(log::ScopedParamContainer &params) const {
    params.add("mountId", mountId);
    params.add("creationTime", creationTime);
    params.add("owner", owner);
  }

  /**
   * Increment Mount Id and return the new value
   *
   * @param txn  Transaction to use for this query
   *
   * @return     Mount ID number
   */
  static uint64_t getNextMountID(Transaction &txn) {
    try {
      const char *const sql = "select NEXTVAL('MOUNT_ID_SEQ') AS MOUNT_ID";
      txn.start();
      auto stmt = txn.conn().createStmt(sql);
      auto rset = stmt.executeQuery();
      if (!rset.next()) {
        throw exception::Exception("Result set is unexpectedly empty");
      }
      return rset.columnUint64("MOUNT_ID");
    } catch (exception::UserError &) {
      throw;
    } catch (exception::Exception &ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + ": " + ex.getMessage().str());
      throw;
    }
  };
};

} // namespace cta::postgresscheddb::sql
