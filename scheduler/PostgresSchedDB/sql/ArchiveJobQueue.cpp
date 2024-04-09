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

#include "scheduler/PostgresSchedDB/sql/ArchiveJobQueue.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::postgresscheddb::sql {

void ArchiveJobQueueRow::updateMountId(Transaction &txn, const std::string& jobIDs, uint64_t mountId) {
  if(jobIDs.empty()) return;

  std::string sql =
    "UPDATE ARCHIVE_JOB_QUEUE SET "
      "MOUNT_ID = :MOUNT_ID "
    "WHERE "
    " JOB_ID IN (" + jobIDs + ")";

  auto stmt = txn.conn().createStmt(sql);
  stmt.bindUint64(":MOUNT_ID", mountId);
  stmt.executeQuery();
}

} // namespace cta::postgresscheddb::sql
