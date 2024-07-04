/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2021-2022 CERN
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

#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {

rdbms::Rset ArchiveJobQueueRow::updateMountInfo(Transaction &txn, ArchiveJobStatus status, const std::string& tapepool, uint64_t mountId, const std::string& vid, uint64_t limit){
  /* using write row lock FOR UPDATE for the select statement
   * since it is the same lock used for UPDATE
   */
  std::string sql =
    "WITH SET_SELECTION AS ( "
      "SELECT JOB_ID FROM ARCHIVE_JOB_QUEUE "
    "WHERE TAPE_POOL = :TAPE_POOL "
    "AND STATUS = :STATUS "
    "AND MOUNT_ID IS NULL "
    "ORDER BY PRIORITY DESC, JOB_ID "
    "LIMIT :LIMIT FOR UPDATE) "
    "UPDATE ARCHIVE_JOB_QUEUE SET "
      "MOUNT_ID = :MOUNT_ID,"
      "VID = :VID "
    "FROM SET_SELECTION "
    "WHERE ARCHIVE_JOB_QUEUE.JOB_ID = SET_SELECTION.JOB_ID "
    "RETURNING SET_SELECTION.JOB_ID";
  auto stmt = txn.conn().createStmt(sql);
  stmt.bindString(":TAPE_POOL", tapepool);
  stmt.bindString(":STATUS", to_string(status));
  stmt.bindUint32(":LIMIT", limit);
  stmt.bindUint64(":MOUNT_ID", mountId);
  stmt.bindString(":VID", vid);
  return stmt.executeQuery();
}

void ArchiveJobQueueRow::updateJobStatus(Transaction &txn, ArchiveJobStatus status, const std::list<std::string>& jobIDs){
  if(jobIDs.empty()) {
    return;
  }
  std::string sqlpart;
  for (const auto &piece : jobIDs) sqlpart += piece + ",";
  if (!sqlpart.empty()) { sqlpart.pop_back(); }
  std::string sql =
          "UPDATE ARCHIVE_JOB_QUEUE SET "
          "STATUS = :STATUS "
          "WHERE JOB_ID IN (" + sqlpart + ") ";
  auto stmt = txn.conn().createStmt(sql);
  stmt.bindString(":STATUS", to_string(status));
  stmt.executeNonQuery();
  return;
};

rdbms::Rset ArchiveJobQueueRow::flagReportingJobsByStatus(Transaction &txn, std::list<ArchiveJobStatus> statusList, uint64_t limit) {
  std::string sql =
          "WITH SET_SELECTION AS ( "
          "SELECT JOB_ID FROM ARCHIVE_JOB_QUEUE "
          "WHERE STATUS = ANY(ARRAY[";
  // we can move this to new bindArray method for stmt
  std::vector<std::string> statusVec;
  std::vector<std::string> placeholderVec;
  size_t j = 1;
  for (const auto &jstatus : statusList) {
    statusVec.push_back(to_string(jstatus));
    std::string plch = std::string(":STATUS") + std::to_string(j);
    placeholderVec.push_back(plch);
    sql += plch;
    if (&jstatus != &statusList.back()) {
      sql += std::string(",");
    }
    j++;
  }
  sql +=  "]::ARCHIVE_JOB_STATUS[] AND IS_OWNED IS NULL)  "
          "ORDER BY PRIORITY DESC, JOB_ID  "
          "LIMIT :LIMIT FOR UPDATE) "
          "UPDATE ARCHIVE_JOB_QUEUE SET "
          "IS_OWNED = 1 "
          "FROM SET_SELECTION "
          "WHERE ARCHIVE_JOB_QUEUE.JOB_ID = SET_SELECTION.JOB_ID "
          "RETURNING SET_SELECTION.JOB_ID";
  auto stmt = txn.conn().createStmt(sql);
  // we can move the array binding to new bindArray method for STMT
  size_t sz = statusVec.size();
  for (size_t i = 0; i < sz; ++i) {
    stmt.bindString(placeholderVec[i], statusVec[i]);
  }
  stmt.bindUint64(":LIMIT", limit);
  return stmt.executeQuery();
}

} // namespace cta::schedulerdb::postgres
