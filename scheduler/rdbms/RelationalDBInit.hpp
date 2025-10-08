/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <string>

#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/Timer.hpp"

namespace cta::catalogue {

class Catalogue;
}

namespace cta {
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
class RelationalDBQCR {
  // DatabaseQueueCleanupRunner
public:
  RelationalDBQCR(catalogue::Catalogue& catalogue, RelationalDB& pgs) : m_conn(pgs.getConn()) {}

  void runOnePass(log::LogContext& lc) {

    /* cta::utils::Timer timer;
    // DELETE is implicit transaction in postgresql

    std::string sql = "DELETE FROM ARCHIVE_ACTIVE_QUEUE WHERE STATUS = :STATUS";
    auto stmt = m_conn.createStmt(sql);
    stmt.bindString(":STATUS", to_string(cta::schedulerdb::ArchiveJobStatus::ReadyForDeletion));
    stmt.executeNonQuery();
    try {
      m_conn.commit();
    } catch (exception::Exception &ex) {
      lc.log(log::ERR, "In RelationalDBQCR::runOnePass(): failed to delete rows of ARCHIVE_ACTIVE_QUEUE" +
                     ex.getMessageValue());
      m_conn.rollback();
    }
    auto ndelrows = stmt.getNbAffectedRows();
    auto tdelsec = timer.secs(cta::utils::Timer::resetCounter);
    lc.log(log::INFO, std::string("In RelationalDBQCR::runOnePass(): Deleted ") +
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

class RelationalDBGC {
  // GarbageCollector
public:
  RelationalDBGC(void* pgstuff, catalogue::Catalogue& catalogue) {}

  void runOnePass(log::LogContext& lc) {}
};

class RelationalDBInit {
public:
  RelationalDBInit(const std::string& client_process,
                   const std::string& db_conn_str,
                   log::Logger& log,
                   bool leaveNonEmptyAgentsBehind = false) {
    connStr = db_conn_str;
    clientProc = client_process;
    login = rdbms::Login::parseString(connStr);
    if (login.dbType != rdbms::Login::DBTYPE_POSTGRESQL) {
      std::runtime_error("scheduler dbconnect string must specify postgres.");
    }
  }

  std::unique_ptr<RelationalDB> getSchedDB(catalogue::Catalogue& catalogue, log::Logger& log) {
    const uint64_t nbConns = 20;
    return std::make_unique<RelationalDB>(clientProc, log, catalogue, login, nbConns);
  }

  RelationalDBGC getGarbageCollector(catalogue::Catalogue& catalogue) { return RelationalDBGC(nullptr, catalogue); }

  RelationalDBQCR getQueueCleanupRunner(catalogue::Catalogue& catalogue, RelationalDB& pgs) {
    return RelationalDBQCR(catalogue, pgs);
  }

private:
  std::string connStr;
  std::string clientProc;
  rdbms::Login login;
};

typedef RelationalDBInit SchedulerDBInit_t;
typedef RelationalDB SchedulerDB_t;

}  // namespace cta
