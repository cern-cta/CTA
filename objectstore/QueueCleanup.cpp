/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "QueueCleanup.hpp"

#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/ObjectstoreInstruments.hpp"
#include "objectstore/GenericObject.hpp"
#include "objectstore/Sorter.hpp"

namespace cta::objectstore {

QueueCleanup::QueueCleanup(cta::log::LogContext& lc,
                           SchedulerDatabase& oStoreDb,
                           catalogue::Catalogue& catalogue,
                           int batchSize)
    : m_lc(lc),
      m_catalogue(catalogue),
      m_db(oStoreDb),
      m_batchSize(batchSize) {}

void QueueCleanup::cleanupQueues() {
  cta::common::dataStructures::SecurityIdentity admin;
  // TODO: Check if these parameters make sense, mainly the username
  admin.username = "Queue cleanup";
  admin.host = cta::utils::getShortHostname();

  auto queuesForCleanup = m_db.getRetrieveQueuesCleanupInfo(m_lc);

  auto queueVidSet = std::set<std::string, std::less<>>();
  for (const auto& queue : queuesForCleanup) {
    queueVidSet.insert(queue.vid);
  }

  common::dataStructures::VidToTapeMap vidToTapesMap;
  std::string toReportQueueAddress;

  if (!queueVidSet.empty()) {
    try {
      vidToTapesMap = m_catalogue.Tape()->getTapesByVid(queueVidSet, true);
    } catch (const exception::UserError& ex) {
      log::ScopedParamContainer params(m_lc);
      params.add("exceptionMessage", ex.getMessageValue());
      m_lc.log(log::ERR,
               "In QueueCleanup::cleanupQueues(): failed to read set of tapes from the database. Aborting cleanup.");
      return;  // Unable to proceed from here...
    }

    for (const auto& vid : queueVidSet) {
      if (vidToTapesMap.count(vid) == 0) {
        log::ScopedParamContainer params(m_lc);
        params.add("tapeVid", vid);
        m_lc.log(log::ERR,
                 "In QueueCleanup::cleanupQueues(): failed to find the tape " + vid + " in the database. Skipping it.");
      }
    }
  } else {
    m_lc.log(log::DEBUG, "In QueueCleanup::cleanupQueues(): no queues requested a cleanup.");
    return;
  }

  for (const auto& [queueVid, tapeData] : vidToTapesMap) {
    // Check if tape state is the expected one (PENDING)
    if (tapeData.state != common::dataStructures::Tape::REPACKING_PENDING
        && tapeData.state != common::dataStructures::Tape::BROKEN_PENDING
        && tapeData.state != common::dataStructures::Tape::EXPORTED_PENDING) {
      // Do not cleanup a tape that is not in a X_PENDING state
      log::ScopedParamContainer params(m_lc);
      params.add("tapeVid", queueVid).add("tapeState", common::dataStructures::Tape::stateToString(tapeData.state));
      m_lc.log(log::INFO,
               "In QueueCleanup::cleanupQueues(): Queue has the cleanup flag enabled but is not in the "
               "expected PENDING state. Skipping it.");
      continue;
    }

    log::ScopedParamContainer loopParams(m_lc);
    loopParams.add("tapeVid", queueVid).add("tapeState", common::dataStructures::Tape::stateToString(tapeData.state));

    m_lc.log(log::DEBUG, "In QueueCleanup::cleanupQueues(): Will try to reserve retrieve queue.");
    try {
      toReportQueueAddress = m_db.blockRetrieveQueueForCleanup(queueVid);
    } catch (OStoreDB::RetrieveQueueNotFound& ex) {
      log::ScopedParamContainer paramsExcMsg(m_lc);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      m_lc.log(log::DEBUG,
               "In QueueCleanup::cleanupQueues(): Unable to find the retrieve queue for cleanup. Queue "
               "may have already been deleted. Skipping it.");
      continue;
    } catch (OStoreDB::RetrieveQueueNotReservedForCleanup& ex) {
      log::ScopedParamContainer paramsExcMsg(m_lc);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      m_lc.log(log::DEBUG,
               "In QueueCleanup::cleanupQueues(): Unable to reserve the retrieve queue due to it not "
               "being available for cleanup. Skipping it.");
      continue;
    } catch (OStoreDB::RetrieveQueueAlreadyReserved& ex) {
      log::ScopedParamContainer paramsExcMsg(m_lc);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      m_lc.log(log::DEBUG, "In QueueCleanup::cleanupQueues(): Queue already reserved. Skipping it.");
      continue;
    } catch (cta::exception::Exception& ex) {
      log::ScopedParamContainer paramsExcMsg(m_lc);
      paramsExcMsg.add("exceptionMessage", ex.getMessageValue());
      m_lc.log(log::WARNING,
               "In QueueCleanup::cleanupQueues(): Unable to reserve the retrieve queue for cleanup for "
               "unknown reasons. Skipping it.");
      continue;
    }

    // Transfer all the jobs to a different queue (if there are replicas) or enqueue the requests into the ToReport queue so that the DiskReporter can report the error back to the user.
    while (true) {
      utils::Timer tLoop;
      log::ScopedParamContainer paramsLoopMsg(m_lc);

      auto dbRet = m_db.getNextRetrieveJobsToTransferBatch(queueVid, m_batchSize, m_lc);
      if (dbRet.empty()) {
        break;
      }
      std::list<cta::SchedulerDatabase::RetrieveJob*> jobPtList;
      for (auto& j : dbRet) {
        jobPtList.push_back(j.get());
      }

      double getQueueTime = tLoop.secs(utils::Timer::resetCounter);

      m_db.requeueRetrieveRequestJobs(jobPtList, toReportQueueAddress, m_lc);

      double jobMovingTime = tLoop.secs(utils::Timer::resetCounter);

      cta::telemetry::metrics::ctaObjectstoreCleanupFileCount->Add(dbRet.size());
      paramsLoopMsg.add("numberOfJobsMoved", dbRet.size())
        .add("getQueueTime", getQueueTime)
        .add("jobMovingTime", jobMovingTime)
        .add("tapeVid", queueVid);
      m_lc.log(cta::log::INFO, "In QueueCleanup::cleanupQueues(): Queue jobs moved.");
    }
    cta::telemetry::metrics::ctaObjectstoreCleanupQueueCount->Add(1);

    // If we managed to requeue all the jobs to a different VID remove the ToReport queue
    // If not, remove the CleanUp flag from the ToReport queue so that the Disk reporter
    // can pick it up.
    if (!m_db.trimEmptyToReportQueue(toReportQueueAddress, m_lc)) {
      m_db.unblockRetrieveQueueForCleanup(toReportQueueAddress);
    }

    // Finally, update the tape state out of PENDING
    {
      cta::common::dataStructures::Tape tapeDataRefreshed;

      try {
        auto vidToTapesMapRefreshed =
          m_catalogue.Tape()->getTapesByVid(queueVid);  //throws an exception if the vid is not found on the database
        tapeDataRefreshed = vidToTapesMapRefreshed.at(queueVid);
      } catch (const catalogue::TapeNotFound& ex) {
        log::ScopedParamContainer params(m_lc);
        params.add("tapeVid", queueVid).add("exceptionMessage", ex.getMessageValue());
        m_lc.log(log::WARNING,
                 "In QueueCleanup::cleanupQueues(): Failed to find a tape in the database. Unable to "
                 "update tape state.");
        continue;  // Ignore queue
      }

      // Finally, modify tape state to REPACKING or BROKEN
      // The STATE_REASON set by operator will be preserved, with just an extra message prepended.
      std::optional<std::string> prevReason = tapeDataRefreshed.stateReason;
      try {
        switch (tapeDataRefreshed.state) {
          case common::dataStructures::Tape::REPACKING_PENDING:
            m_catalogue.Tape()->modifyTapeState(admin,
                                                queueVid,
                                                common::dataStructures::Tape::REPACKING,
                                                common::dataStructures::Tape::REPACKING_PENDING,
                                                prevReason.value_or("QueueCleanup: changed tape state to REPACKING"));
            m_db.clearStatisticsCache(queueVid);
            break;
          case common::dataStructures::Tape::BROKEN_PENDING:
            m_catalogue.Tape()->modifyTapeState(admin,
                                                queueVid,
                                                common::dataStructures::Tape::BROKEN,
                                                common::dataStructures::Tape::BROKEN_PENDING,
                                                prevReason.value_or("QueueCleanup: changed tape state to BROKEN"));
            m_db.clearStatisticsCache(queueVid);
            break;
          case common::dataStructures::Tape::EXPORTED_PENDING:
            m_catalogue.Tape()->modifyTapeState(admin,
                                                queueVid,
                                                common::dataStructures::Tape::EXPORTED,
                                                common::dataStructures::Tape::EXPORTED_PENDING,
                                                prevReason.value_or("QueueCleanup: changed tape state to EXPORTED"));
            m_db.clearStatisticsCache(queueVid);
            break;
          default:
            log::ScopedParamContainer paramsWarnMsg(m_lc);
            paramsWarnMsg.add("tapeVid", queueVid)
              .add("expectedPrevState", common::dataStructures::Tape::stateToString(tapeData.state))
              .add("actualPrevState", common::dataStructures::Tape::stateToString(tapeDataRefreshed.state));
            m_lc.log(log::WARNING,
                     "In QueueCleanup::cleanupQueues(): Cleaned up tape is not in a PENDING state. Unable to "
                     "change it to its corresponding final state.");
            break;
        }
      } catch (const catalogue::UserSpecifiedAWrongPrevState& ex) {
        using common::dataStructures::Tape;
        auto tapeDataRefreshedUpdated = m_catalogue.Tape()->getTapesByVid(queueVid).at(queueVid);
        log::ScopedParamContainer paramsWarnMsg(m_lc);
        paramsWarnMsg.add("tapeVid", queueVid)
          .add("expectedPrevState", common::dataStructures::Tape::stateToString(tapeDataRefreshed.state))
          .add("actualPrevState", common::dataStructures::Tape::stateToString(tapeDataRefreshedUpdated.state));
        if ((tapeDataRefreshed.state == Tape::REPACKING_PENDING && tapeDataRefreshedUpdated.state == Tape::REPACKING)
            || (tapeDataRefreshed.state == Tape::BROKEN_PENDING && tapeDataRefreshedUpdated.state == Tape::BROKEN)
            || (tapeDataRefreshed.state == Tape::EXPORTED_PENDING
                && tapeDataRefreshedUpdated.state == Tape::EXPORTED)) {
          m_lc.log(log::WARNING,
                   "In QueueCleanup::cleanupQueues(): Tape already moved into it's final state, probably by "
                   "another agent.");
        } else {
          m_lc.log(log::ERR,
                   "In QueueCleanup::cleanupQueues(): Tape moved into an unexpected final state, probably by "
                   "another agent.");
        }
      }
    }
  }
}

}  // namespace cta::objectstore
