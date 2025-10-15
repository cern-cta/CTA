/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#pragma once

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "maintenance/IMaintenanceRunner.hpp"
#include "rdbms/Conn.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

namespace cta::maintenance {

/**
 * The RelationalDBQCR or RelationalDBGC (TO BE DECIDED) should regularly look at the ARCHIVE_PENDING_QUEUE for
 * any jobs with assigned MOUNT_ID for which there are no active MOUNTS and sets their
 * MOUNT_ID to NULL  freeing them to be requeued to new drive queues.
 * In addition, it checks the ARCHIVE_ACTIVE_QUEUE table where the drive queues are
 * and checks if there are any jobs for which there was no update since a very long period of time.
 * In that case, it looks up the status of the mount and if active, does nothing,
 * if dead then it cleans the task queue
 * and moved the jobs back to ARCHIVE_PENDING_QUEUE so that they can be queued again.
 */
class RelationalDBQCR : public IMaintenanceRunner {
  // DatabaseQueueCleanupRunner
public:
  RelationalDBQCR(catalogue::Catalogue& catalogue, RelationalDB& pgs) : m_conn(pgs.getConn()) {}

  void executeRunner(log::LogContext& lc) {

    /* cta::utils::Timer timer;
    // DELETE is implicit transaction in postgresql

    std::string sql = "DELETE FROM ARCHIVE_ACTIVE_QUEUE WHERE STATUS = :STATUS";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindString(":STATUS", to_string(cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion));
    stmt.executeNonQuery();
    try {
      m_conn.commit();
    } catch (exception::Exception &ex) {
      lc.log(log::ERR, "In RelationalDBQCR::executeRunner(): failed to delete rows of ARCHIVE_ACTIVE_QUEUE" +
                     ex.getMessageValue());
      m_conn.rollback();
    }
    auto ndelrows = stmt.getNbAffectedRows();
    auto tdelsec = timer.secs(cta::utils::Timer::resetCounter);
    lc.log(log::INFO, std::string("In RelationalDBQCR::executeRunner(): Deleted ") +
                      std::to_string(ndelrows) +
                      std::string(" rows from the ARCHIVE_ACTIVE_QUEUE. Operation took ") +
                      std::to_string(tdelsec) + std::string(" seconds."));
    */
    /* Autovacuum might be heavy and degrading performance
     * optionally we can partition the table and drop partitions,
     * this drop operation locks only the specific partition
     * without affecting other parts of the table
     * 1. get the current time stamps in epoch and check existing partitions
     * 2. create new partitions if needed
     * 3. check and drop partitions which are ready to be dropped
     * SELECT
     * COUNT(*) = COUNT(CASE WHEN status = 'completed' THEN 1 END) AS all_rows_completed
     * FROM
     * partition_name;  -- Replace partition_name with the actual name
     */
  }

private:
  rdbms::Conn m_conn;
};

}
    
