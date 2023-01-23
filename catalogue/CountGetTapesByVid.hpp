/*
 * @project	 The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
 * @license	 This program is free software, distributed under the terms of the GNU General Public
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

#include "common/threading/MutexLocker.hpp"
#include <atomic>

namespace cta {
namespace catalogue {
namespace countGetTapesByVid {

enum Enum {
  RDBMS_GETTAPESBYVID,
  SBRQ_GCOOS,
  SBRQ_GCRR,
  SBRQ_SRT,
  SBRQ_QR,
  SBRQ_ASAUS1,
  SBRQ_ASAUS2,
  SBRQ_FT1,
  SBRQ_FT2,
  SBRQ_FT2R,
  QCR1,
  QCR2,
  FMILCK,
  FMIUNLCK,
  FCTCBR,
  TTSC,
  CS,
  GT,
  EMAX
};

static struct Logstr {
  Enum loccode;
  const char *logstr;
} logstr[]  __attribute__((__unused__)) = {
               {RDBMS_GETTAPESBYVID, "RdbmsCatalogue::getTapesByVid" },
               {SBRQ_GCOOS, "RetrieveRequest::garbageCollectRetrieveRequest" },
               {SBRQ_SRT, "Sorter::getBestVidForQueueingRetrieveRequest" },
               {SBRQ_QR, "OStoreDB::queueRetrieve" },
               {SBRQ_ASAUS1, "OStoreDB::RepackRequest::addSubrequestsAndUpdateStats_1" },
               {SBRQ_ASAUS2, "OStoreDB::RepackRequest::addSubrequestsAndUpdateStats_2" },
               {SBRQ_FT1, "OStoreDB::RetrieveJob::failTransfer_1" },
               {SBRQ_FT2, "OStoreDB::RetrieveJob::failTransfer_2" },
               {SBRQ_FT2R, "OStoreDB::RetrieveJob::failTransfer_2R" },
               {QCR1, "QueueCleanupRunner::runOnePass_1" },
               {QCR2, "QueueCleanupRunner::runOnePass_2" },
               {FMILCK, "OStoreDB::fetchMountInfo_locked" },
               {FMIUNLCK, "OStoreDB::fetchMountInfo_unlocked" },
               {FCTCBR, "Scheduler::checkTapeCanBeRepacked" },
               {TTSC, "Scheduler::triggerTapeStateChange" },
               {CS, "castor::tape::tapeserver::daemon::CleanerSession::exceptionThrowingExecute" },
               {GT, "Scheduler::sortAndGetTapesForMountInfo_getTapes()" }
    };

struct CounterSpace {
  threading::Mutex counterMutex{std::vector<int>{PTHREAD_PROCESS_SHARED}};
  std::atomic<double> getTapesByVidCounterTime = std::atomic<double>(0.);
  std::atomic<uint64_t> getTapesByVidCounters[countGetTapesByVid::EMAX] = { std::atomic<uint64_t>(0) };
};

constexpr double minReportInterval = 600.;

}
}
}
