/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::schedulerdb::postgres {

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
  explicit MountsRow(const rdbms::Rset& rset) { *this = rset; }

  MountsRow& operator=(const rdbms::Rset& rset) {
    mountId = rset.columnUint64("MOUNT_ID");
    creationTime = rset.columnUint64("CREATION_TIMESTAMP");
    owner = rset.columnString("OWNER");
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
   * @param txn  Transaction to use for this query
   *
   * @return     Mount ID number
   */
  static uint64_t getNextMountID(Transaction& txn) {
    const char* const sql = R"SQL(
      SELECT NEXTVAL('MOUNT_ID_SEQ') AS MOUNT_ID
    )SQL";
    auto stmt = txn.getConn().createStmt(sql);
    auto rset = stmt.executeQuery();
    if (!rset.next()) {
      throw exception::Exception("Result set is unexpectedly empty");
    }
    return rset.columnUint64("MOUNT_ID");
  };
};

}  // namespace cta::schedulerdb::postgres
