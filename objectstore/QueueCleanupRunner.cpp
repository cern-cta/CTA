/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "objectstore/QueueCleanupRunner.hpp"

namespace cta::objectstore {

QueueCleanupRunner::QueueCleanupRunner(AgentReference &agentReference, SchedulerDatabase & oStoreDb, catalogue::Catalogue &catalogue,
                                       std::optional<double> heartBeatTimeout, std::optional<int> batchSize) :
        m_catalogue(catalogue), m_db(oStoreDb),
        m_batchSize(batchSize.value_or(DEFAULT_BATCH_SIZE)), m_heartBeatTimeout(heartBeatTimeout.value_or(DEFAULT_HEARTBEAT_TIMEOUT)) {
}

void QueueCleanupRunner::runOnePass(log::LogContext &logContext) {

  cta::common::dataStructures::SecurityIdentity admin;
  // TODO: Check if these parameters make sense, mainly the username
  admin.username = "Queue cleanup runner";
  admin.host = cta::utils::getShortHostname();

  auto queueVidSet = std::set<std::string>();
  auto queuesForCleanup = m_db.getRetrieveQueuesCleanupInfo(logContext);

  // Check, one-by-one, queues need to be cleaned up
  for (const auto &queue: queuesForCleanup) {

    // Do not clean a queue that does not have the cleanup flag set true
    if (!queue.doCleanup) {
      continue; // Ignore queue
    }

    // Check heartbeat of queues to know if they are being cleaned up
    if (queue.assignedAgent.has_value()) {
      if ((m_heartbeatCheck.count(queue.vid) == 0) || (m_heartbeatCheck[queue.vid].heartbeat != queue.heartbeat)) {
        // If this queue was never seen before, wait for another turn to check if its heartbeat has timed out.
        // If heartbeat has been updated, then the queue is being actively processed by another agent.
        // Record new timestamp and move on.
        m_heartbeatCheck[queue.vid].agent = queue.assignedAgent.value();
        m_heartbeatCheck[queue.vid].heartbeat = queue.heartbeat;
        m_heartbeatCheck[queue.vid].lastUpdateTimestamp = m_timer.secs();
        continue; // Ignore queue
      } else {
        // If heartbeat has not been updated, check how long ago the last update happened
        // If not enough time has passed, do not consider this queue for cleanup
        auto lastHeartbeatTimestamp = m_heartbeatCheck[queue.vid].lastUpdateTimestamp;
        if ((m_timer.secs() - lastHeartbeatTimestamp) < m_heartBeatTimeout) {
          continue; // Ignore queue
        }
      }
    }
    queueVidSet.insert(queue.vid);
  }

  common::dataStructures::VidToTapeMap vidToTapesMap;

  if (!queueVidSet.empty()){
    try {
      vidToTapesMap = m_catalogue.Tape()->getTapesByVid(queueVidSet, true);
    } catch (const exception::UserError &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::ERR,
                     "ERROR: In QueueCleanupRunner::runOnePass(): failed to read set of tapes from the database. Aborting cleanup.");
      return; // Unable to proceed from here...
    }

    for (const auto &vid: queueVidSet) {
      if (vidToTapesMap.count(vid) == 0) {
        log::ScopedParamContainer params(logContext);
        params.add("tapeVid", vid);
        logContext.log(log::ERR,
                       "ERROR: In QueueCleanupRunner::runOnePass(): failed to find the tape " + vid + " in the database. Skipping it.");
      }
    }
  } else {
    logContext.log(log::DEBUG,
                   "DEBUG: In QueueCleanupRunner::runOnePass(): no queues requested a cleanup.");
    return;
  }

  for (const auto &[queueVid, tapeData]: vidToTapesMap) {

    // Check if tape state is the expected one (PENDING)
    if (tapeData.state != common::dataStructures::Tape::REPACKING_PENDING
        && tapeData.state != common::dataStructures::Tape::BROKEN_PENDING
        && tapeData.state != common::dataStructures::Tape::EXPORTED_PENDING) {
      // Do not cleanup a tape that is not in a X_PENDING state
      log::ScopedParamContainer params(logContext);
      params.add("tapeVid", queueVid)
            .add("tapeState", common::dataStructures::Tape::stateToString(tapeData.state));
      logContext.log(
              log::INFO,
              "In QueueCleanupRunner::runOnePass(): Queue is has cleanup flag enabled but is not in the expected PENDING state. Skipping it.");
      continue;
    }

    log::ScopedParamContainer loopParams(logContext);
    loopParams.add("tapeVid", queueVid)
              .add("tapeState", common::dataStructures::Tape::stateToString(tapeData.state));

    try {
      bool prevHeartbeatExists = (m_heartbeatCheck.find(queueVid) != m_heartbeatCheck.end());
      m_db.reserveRetrieveQueueForCleanup(
              queueVid,
              prevHeartbeatExists ? std::optional(m_heartbeatCheck[queueVid].heartbeat) : std::nullopt);
    } catch (OStoreDB::RetrieveQueueNotFound & ex) {
      log::ScopedParamContainer paramsExcMsg(logContext);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG,
                     "In QueueCleanupRunner::runOnePass(): Unable to find the retrieve queue for cleanup. Queue may have already been deleted. Skipping it.");
      continue;
    } catch (OStoreDB::RetrieveQueueNotReservedForCleanup & ex) {
      log::ScopedParamContainer paramsExcMsg(logContext);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::DEBUG,
                     "In QueueCleanupRunner::runOnePass(): Unable to reserve the retrieve queue due to it not being available for cleanup. Skipping it.");
      continue;
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer paramsExcMsg(logContext);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      logContext.log(log::WARNING,
                     "In QueueCleanupRunner::runOnePass(): Unable to reserve the retrieve queue for cleanup for unknown reasons. Skipping it.");
      continue;
    }

    // Transfer all the jobs to a different queue (if there are replicas) or report the error back to the user
    while (true) {

      utils::Timer tLoop;
      log::ScopedParamContainer paramsLoopMsg(logContext);

      auto dbRet = m_db.getNextRetrieveJobsToTransferBatch(queueVid, m_batchSize, logContext);
      if (dbRet.empty()) break;
      std::list<cta::SchedulerDatabase::RetrieveJob *> jobPtList;
      for (auto &j: dbRet) {
        jobPtList.push_back(j.get());
      }
      m_db.requeueRetrieveRequestJobs(jobPtList, logContext);

      double jobMovingTime = tLoop.secs(utils::Timer::resetCounter);

      paramsLoopMsg.add("numberOfJobsMoved", dbRet.size())
                   .add("jobMovingTime", jobMovingTime)
                   .add("tapeVid", queueVid);
      logContext.log(cta::log::INFO,"In DiskReportRunner::runOnePass(): Queue jobs moved.");

      // Tick heartbeat
      try {
        m_db.tickRetrieveQueueCleanupHeartbeat(queueVid);
      } catch (OStoreDB::RetrieveQueueNotFound & ex) {
        break; // Queue was already deleted, probably after all the requests have been removed
      } catch (OStoreDB::RetrieveQueueNotReservedForCleanup & ex) {
        log::ScopedParamContainer paramsExcMsg(logContext);
        paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
        logContext.log(log::WARNING,
                       "In QueueCleanupRunner::runOnePass(): Unable to update heartbeat of retrieve queue cleanup, due to it not being reserved by agent. Aborting cleanup.");
        break;
      } catch (cta::exception::Exception & ex) {
        log::ScopedParamContainer paramsExcMsg(logContext);
        paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
        logContext.log(log::WARNING,
                       "In QueueCleanupRunner::runOnePass(): Unable to update heartbeat of retrieve queue cleanup for unknown reasons. Aborting cleanup.");
        break;
      }
    }

    // Finally, update the tape state out of PENDING
    {
      cta::common::dataStructures::Tape tapeDataRefreshed;

      try {
        auto vidToTapesMapRefreshed = m_catalogue.Tape()->getTapesByVid(queueVid); //throws an exception if the vid is not found on the database
        tapeDataRefreshed = vidToTapesMapRefreshed.at(queueVid);
      } catch (const exception::UserError &ex) {
        log::ScopedParamContainer params(logContext);
        params.add("tapeVid", queueVid)
              .add("exceptionMessage", ex.getMessageValue());
        logContext.log(log::WARNING, "WARNING: In QueueCleanupRunner::runOnePass(): Failed to find a tape in the database. Unable to update tape state.");
        continue; // Ignore queue
      }

      // Finally, modify tape state to REPACKING or BROKEN
      // The STATE_REASON set by operator will be preserved, with just an extra message prepended.
      std::optional<std::string> prevReason = tapeDataRefreshed.stateReason;
      switch (tapeDataRefreshed.state) {
      case common::dataStructures::Tape::REPACKING_PENDING:
        m_catalogue.Tape()->modifyTapeState(admin, queueVid, common::dataStructures::Tape::REPACKING, common::dataStructures::Tape::REPACKING_PENDING, prevReason.value_or("QueueCleanupRunner: changed tape state to REPACKING"));
        m_db.clearRetrieveQueueStatisticsCache(queueVid);
        break;
      case common::dataStructures::Tape::BROKEN_PENDING:
        m_catalogue.Tape()->modifyTapeState(admin, queueVid, common::dataStructures::Tape::BROKEN, common::dataStructures::Tape::BROKEN_PENDING, prevReason.value_or("QueueCleanupRunner: changed tape state to BROKEN"));
        m_db.clearRetrieveQueueStatisticsCache(queueVid);
        break;
      case common::dataStructures::Tape::EXPORTED_PENDING:
        m_catalogue.Tape()->modifyTapeState(admin, queueVid, common::dataStructures::Tape::EXPORTED, common::dataStructures::Tape::EXPORTED_PENDING, prevReason.value_or("QueueCleanupRunner: changed tape state to EXPORTED"));
        m_db.clearRetrieveQueueStatisticsCache(queueVid);
        break;
      default:
        log::ScopedParamContainer paramsWarnMsg(logContext);
        paramsWarnMsg.add("tapeVid", queueVid)
                     .add("expectedPrevState", common::dataStructures::Tape::stateToString(tapeData.state))
                     .add("actualPrevState", common::dataStructures::Tape::stateToString(tapeDataRefreshed.state));
        logContext.log(log::WARNING, "WARNING: In QueueCleanupRunner::runOnePass(): Cleaned up tape is not in a PENDING state. Unable to change it to its corresponding final state.");
        break;
      }
    }
  }
}

} // namespace cta::objectstore
