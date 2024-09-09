/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright	   Copyright © 2023 CERN
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

#include "scheduler/rdbms/postgres/RetrieveJobQueue.hpp"
#include "rdbms/wrapper/PostgresColumn.hpp"
#include "rdbms/wrapper/PostgresStmt.hpp"

namespace cta::schedulerdb::postgres {

void RetrieveJobQueueRow::updateMountID(Transaction &txn, const std::list<RetrieveJobQueueRow>& rowList, uint64_t mountId) {
  if(rowList.empty()) return;

  try {
    const char* const sqltt = R"SQL(
      CREATE TEMPORARY TABLE TEMP_JOB_IDS (JOB_ID BIGINT) ON COMMIT DROP
    )SQL";
    txn.getConn().executeNonQuery(sqltt);
  } catch(exception::Exception &ex) {
    const char* const sqltrunc = R"SQL(
      TRUNCATE TABLE TEMP_JOB_IDS
    )SQL";
    txn.getConn().executeNonQuery(sqltrunc);
  }

  const char* const sqlcopy = R"SQL(
    COPY TEMP_JOB_IDS(JOB_ID) FROM STDIN --:JOB_ID
  )SQL";

  auto stmt = txn.getConn().createStmt(sqlcopy);
  auto & postgresStmt = dynamic_cast<rdbms::wrapper::PostgresStmt &>(stmt.getStmt());

  const size_t nbrows = rowList.size();
  cta::rdbms::wrapper::PostgresColumn c1("JOB_ID", nbrows);
  std::list<RetrieveJobQueueRow>::const_iterator itr;
  size_t i;
  for(i=0,itr=rowList.begin();i<nbrows;++i,++itr) {
    c1.setFieldValue(i, std::to_string(itr->jobId));
  }

  postgresStmt.setColumn(c1);
  postgresStmt.executeCopyInsert(nbrows);

  const char* const sql = R"SQL(
    UPDATE RETRIEVE_JOB_QUEUE SET 
      MOUNT_ID = :MOUNT_ID 
    WHERE 
       JOB_ID IN (SELECT JOB_ID FROM TEMP_JOB_IDS)
  )SQL";

  stmt = txn.getConn().createStmt(sql);
  stmt.bindUint64(":MOUNT_ID", mountId);
  stmt.executeQuery();
}

} // namespace cta::schedulerdb::postgres
