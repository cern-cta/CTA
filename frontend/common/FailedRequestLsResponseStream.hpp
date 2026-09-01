/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Rset.hpp"

#include <deque>
#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class FailedRequestLsResponseStream final : public CtaAdminResponseStream {
public:
  FailedRequestLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                cta::Scheduler& scheduler,
                                const std::string& instanceName,
                                const admin::AdminCmd& adminCmd,
                                SchedulerDatabase& schedDb,
                                cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  SchedulerDatabase& m_schedDb;
  cta::log::LogContext& m_lc;

  // Configuration options
  bool m_isSummary = false;
  bool m_isLogEntries = false;
  std::optional<std::string> m_schedulerBackendName;

  // Data storage
  std::list<cta::xrd::Data> m_summaryData;  // Only 3 items max
#ifdef CTA_PGSCHED
  /**
   * The failed queues listed by this stream. The user and the repack jobs are kept in separate
   * tables, the repack ones in the tables with the REPACK_ name prefix. The table a job was read
   * from is the only indication that it is a repack job, hence the queue being read is tracked
   * in order to prefix the reported object IDs accordingly.
   */
  enum class FailedQueue { UserArchive, RepackArchive, UserRetrieve, RepackRetrieve };

  static bool isArchiveQueue(FailedQueue failedQueue);
  static bool isRepackQueue(FailedQueue failedQueue);

  /**
   * The connection the failed queues are read from. Declared before the result sets below so
   * that it is destroyed after them: a result set outliving its connection would be left
   * pointing at a connection already handed back to the pool.
   */
  std::unique_ptr<cta::rdbms::Conn> m_conn;

  /**
   * The failed queues still to be listed, in the order they are reported. A Postgres connection
   * can only have a single query in flight at a time, hence one queue is queried at a time, each
   * one as the previous one gets exhausted.
   */
  std::deque<FailedQueue> m_queuesToList;
  FailedQueue m_currentQueue = FailedQueue::UserArchive;
  std::unique_ptr<rdbms::Rset> m_currentRows;
  bool m_hasNext = false;

  // Filters to apply to the queues which have not been queried yet
  std::optional<std::string> m_tapePool;
  std::optional<std::string> m_vid;

  void fillCommonFields(cta::admin::FailedRequestLsItem& fr_item, const cta::rdbms::Rset& item);

  /**
   * Query one failed queue
   *
   * @param failedQueue  the failed queue to query
   *
   * @return the rows of the queue, never a null pointer
   */
  std::unique_ptr<rdbms::Rset> queryFailedQueue(FailedQueue failedQueue);

  /**
   * Makes the next row available for reporting, moving on to the following failed queue whenever
   * the current one is exhausted. Leaves m_hasNext false once all the queues have been reported.
   */
  void advanceToNextRow();
#else
  std::unique_ptr<SchedulerDatabase::IArchiveJobQueueItor> m_archiveJobQueueItorPtr;
  std::unique_ptr<SchedulerDatabase::IRetrieveJobQueueItor> m_retrieveJobQueueItorPtr;
#endif
  // Helper methods
  cta::xrd::Data getNextArchiveJobsData();
  cta::xrd::Data getNextRetrieveJobsData();

  void collectSummaryData(bool hasArchive, bool hasRetrieve);
};

}  // namespace cta::frontend
