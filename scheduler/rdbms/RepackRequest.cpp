/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RepackRequest.hpp"

#include "common/exception/NotImplementedException.hpp"
#include "common/log/LogContext.hpp"
#include "rdbms/UniqueConstraintError.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/rdbms/Helpers.hpp"
#include "scheduler/rdbms/RetrieveRequest.hpp"

#include <algorithm>

namespace cta::schedulerdb {

uint64_t RepackRequest::getLastExpandedFSeq() {
  return repackInfo.lastExpandedFseq;
}

void RepackRequest::setLastExpandedFSeq(uint64_t fseq) {
  repackInfo.lastExpandedFseq = fseq;
}

void RepackRequest::setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles) {
  repackInfo.totalFilesOnTapeAtStart = totalStatsFiles.totalFilesOnTapeAtStart;
  repackInfo.totalBytesOnTapeAtStart = totalStatsFiles.totalBytesOnTapeAtStart;
  repackInfo.allFilesSelectedAtStart = totalStatsFiles.allFilesSelectedAtStart;
  repackInfo.totalFilesToRetrieve = totalStatsFiles.totalFilesToRetrieve;
  repackInfo.totalFilesToArchive = totalStatsFiles.totalFilesToArchive;
  repackInfo.totalBytesToArchive = totalStatsFiles.totalBytesToArchive;
  repackInfo.totalBytesToRetrieve = totalStatsFiles.totalBytesToRetrieve;
  repackInfo.userProvidedFiles = totalStatsFiles.userProvidedFiles;
}

void RepackRequest::reportRetrieveCreationFailures(const uint64_t failedToRetrieveFiles,
                                                   const uint64_t failedToRetrieveBytes,
                                                   const uint64_t failedArchiveReq) {
  m_failedToCreateArchiveReq += failedArchiveReq;
  repackInfo.failedFilesToRetrieve += failedToRetrieveFiles;
  repackInfo.failedBytesToRetrieve += failedToRetrieveBytes;
  auto newStatus = getCurrentStatus();
  repackInfo.status = newStatus;
  cta::schedulerdb::Transaction txn(m_connPool, m_lc);
  try {
    uint64_t nrows = postgres::RepackRequestTrackingRow::updateRepackRequestFailures(
      txn,
      repackInfo.repackReqId,
      failedToRetrieveFiles,
      failedToRetrieveBytes,
      failedArchiveReq,
      mapRepackInfoStatusToJobStatus(repackInfo.status));
    log::ScopedParamContainer(m_lc)
      .add("nrows", nrows)
      .log(log::INFO,
           "In RepackRequest::reportRetrieveCreationFailures(): updateRepackRequestFailures() called successfully "
           "after expansion.");
    txn.commit();
  } catch (cta::exception::Exception& e) {
    std::string bt = e.backtrace();
    m_lc.log(log::ERR,
             "In RepackRequest::reportRetrieveCreationFailures(): updateRepackRequestFailures() Exception thrown: "
               + bt);
    txn.abort();
  }
}

uint64_t
RepackRequest::addSubrequestsAndUpdateStats(const std::list<Subrequest>& repackSubrequests,
                                            const cta::common::dataStructures::ArchiveRoute::FullMap& archiveRoutesMap,
                                            uint64_t maxFSeqLowBound,
                                            const uint64_t maxAddedFSeq,
                                            const TotalStatsFiles& totalStatsFiles,
                                            const disk::DiskSystemList& diskSystemList,
                                            log::LogContext& lc) {
  uint64_t nbRetrieveSubrequestsCreated = 0;

  // Map existing subrequests by fSeq
  std::map<uint64_t, SubrequestPointer*> srmap;
  for (auto& r : m_subreqp) {
    srmap[r.fSeq] = &r;
  }

  // Add new subrequests if needed
  for (auto& r : repackSubrequests) {
    lc.log(log::DEBUG, "In RepackRequest::addSubrequestsAndUpdateStats(): repackSubrequests ");
    if (!srmap.contains(r.fSeq)) {
      m_subreqp.emplace_back();
      auto& newSub = m_subreqp.back();
      newSub.archiveCopyNbsSet = r.copyNbsToRearchive;
      newSub.fSeq = r.fSeq;
      srmap[r.fSeq] = &newSub;
    }
  }

  setTotalStats(totalStatsFiles);
  uint64_t fSeq = std::max(maxFSeqLowBound + 1, maxAddedFSeq + 1);
  bool noRecall = repackInfo.noRecall;

  StatsValues failedCreationStats;
  uint64_t failedArchiveReq = 0;

  auto subReqItor = repackSubrequests.begin();
  while (subReqItor != repackSubrequests.end()) {
    uint64_t nbSubReqProcessed = 0;
    std::vector<std::unique_ptr<postgres::RetrieveJobQueueRow>> rrRowBatchToTransfer;
    std::vector<std::unique_ptr<postgres::RetrieveJobQueueRow>> rrRowBatchNoRecall;
    while (subReqItor != repackSubrequests.end() && nbSubReqProcessed < 500) {
      // Requests marked as deleted are guaranteed to have already been created => we will not re-attempt.
      if (auto& rsr = *subReqItor; !srmap.at(rsr.fSeq)->isSubreqDeleted) {
        try {
          auto conn = m_connPool.getConn();
          RetrieveRequest rr(conn, lc);

          // Set scheduler request
          common::dataStructures::RetrieveRequest schedReq;
          schedReq.archiveFileID = rsr.archiveFile.archiveFileID;
          schedReq.dstURL = rsr.fileBufferURL;
          schedReq.appendFileSizeToDstURL(rsr.archiveFile.fileSize);
          log::ScopedParamContainer(m_lc)
            .add("diskFileInfo.path", rsr.archiveFile.diskFileInfo.path)
            .add("archiveFile.fileSize", rsr.archiveFile.fileSize)
            .log(log::DEBUG, "In RepackRequest::addSubrequestsAndUpdateStats(): diskFileInfo.path ?");
          schedReq.diskFileInfo = rsr.archiveFile.diskFileInfo;
          rr.setSchedulerRequest(schedReq);

          // Disk system
          try {
            m_lc.log(log::DEBUG,
                     "In RepackRequest::addSubrequestsAndUpdateStats(): Extracting diskSystemName from :"
                       + schedReq.dstURL);
            rr.setDiskSystemName(diskSystemList.getDSName(schedReq.dstURL));
          } catch (std::out_of_range&) {
            m_lc.log(log::DEBUG,
                     "In RepackRequest::addSubrequestsAndUpdateStats(): Extracting diskSystemName threw fake "
                     "out_of_range exception no disk system from the list matched.");
          }

          // Set repack info
          RetrieveRequest::RetrieveReqRepackInfo rRRepackInfo;
          rRRepackInfo.fSeq = rsr.fSeq;
          rRRepackInfo.fileBufferURL = rsr.fileBufferURL;
          rRRepackInfo.copyNbsToRearchive = rsr.copyNbsToRearchive;
          rRRepackInfo.isRepack = true;
          rRRepackInfo.repackRequestId = repackInfo.repackReqId;
          rRRepackInfo.hasUserProvidedFile = rsr.hasUserProvidedFile;

          std::ostringstream oss;
          for (auto nb : rRRepackInfo.copyNbsToRearchive) {
            if (oss.tellp() > 0) {
              oss << ",";
            }
            oss << nb;
          }
          log::ScopedParamContainer(m_lc)
            .add("copyNbsToRearchive_raw", oss.str())
            .log(log::DEBUG, "Pre-makeJobRow() raw copyNbsToRearchive set.");

          // Archive route map
          try {
            for (auto& ar : archiveRoutesMap.at(rsr.archiveFile.storageClass)) {
              rRRepackInfo.archiveRouteMap[ar.second.copyNb] = ar.second.tapePoolName;
            }
            //Check that we do not have the same destination tapepool for two different copyNb
            for (auto& currentCopyNbTapePool : rRRepackInfo.archiveRouteMap) {
              int nbTapepool =
                std::count_if(rRRepackInfo.archiveRouteMap.begin(),
                              rRRepackInfo.archiveRouteMap.end(),
                              [&currentCopyNbTapePool](const std::pair<uint64_t, std::string>& copyNbTapepool) {
                                return copyNbTapepool.second == currentCopyNbTapePool.second;
                              });
              if (nbTapepool != 1) {
                throw cta::ExpandRepackRequestException("In RepackRequest::addSubrequestsAndUpdateStats(), "
                                                        "found the same destination tapepool for different copyNb.");
              }
            }
          } catch (std::out_of_range&) {
            failedCreationStats.files++;
            failedCreationStats.bytes += rsr.archiveFile.fileSize;
            failedArchiveReq += rsr.copyNbsToRearchive.size();
            m_lc.log(log::ERR, "Archive route not found for subrequest");
            ++subReqItor;
            ++nbSubReqProcessed;
            std::stringstream storageClassList;
            bool first = true;
            for (auto& sc : archiveRoutesMap) {
              std::string storageClass;
              storageClass = sc.first;
              storageClassList << (first ? "" : " ") << " sc=" << storageClass << " rc=" << sc.second.size();
            }
            log::ScopedParamContainer(m_lc)
              .add("fileID", rsr.archiveFile.archiveFileID)
              .add("diskInstance", rsr.archiveFile.diskInstance)
              .add("storageClass", rsr.archiveFile.storageClass)
              .add("storageClassList", storageClassList.str())
              .log(log::ERR, "In RepackRequest::addSubrequestsAndUpdateStats(): not such archive route.");
            continue;
          }

          rr.setRepackInfo(rRRepackInfo);

          // Set queueing criteria
          common::dataStructures::RetrieveFileQueueCriteria fileQueueCriteria;
          fileQueueCriteria.archiveFile = rsr.archiveFile;
          fileQueueCriteria.mountPolicy = m_mountPolicy;
          rr.fillJobsSetRetrieveFileQueueCriteria(fileQueueCriteria);

          // Decide from which VID we are going to retrieve the file. Here, if we can retrieve from the repack VID, we
          // will set the initial recall on it. Retries will we requeue to best VID as usual if needed.
          std::string bestVid;
          uint32_t activeCopyNumber = 0;

          if (noRecall) {
            bestVid = repackInfo.vid;
          } else {
            //No --no-recall flag, make sure we have a copy on the vid we intend to repack.
            for (auto& tc : rsr.archiveFile.tapeFiles) {
              if (tc.vid == repackInfo.vid) {
                try {
                  auto conn = m_connPool.getConn();
                  Helpers::selectBestVid4Retrieve({repackInfo.vid}, m_catalogue, conn, true);
                  bestVid = repackInfo.vid;
                  activeCopyNumber = tc.copyNb;
                } catch (Helpers::NoTapeAvailableForRetrieve&) {
                  log::ScopedParamContainer(m_lc)
                    .add("VID", repackInfo.vid)
                    .log(log::WARNING,
                         "In RepackRequest::addSubrequestsAndUpdateStats(): Tape VID not available for retrieve at the "
                         "moment.");
                }
                break;
              }
            }
          }
          bool foundCopyNb = false;
          for (auto& tc : rsr.archiveFile.tapeFiles) {
            if (tc.vid == bestVid) {
              activeCopyNumber = tc.copyNb;
              foundCopyNb = true;
              break;
            }
          }
          if (!foundCopyNb) {
            log::ScopedParamContainer(m_lc)
              .add("fileId", rsr.archiveFile.archiveFileID)
              .add("repackVid", repackInfo.vid)
              .add("chosenVid", bestVid)
              .log(log::ERR,
                   "In RepackRequest::addSubrequestsAndUpdateStats(): could not find the copyNb "
                   "for the chosen VID. Subrequest failed.");
            failedCreationStats.files++;
            failedCreationStats.bytes += rsr.archiveFile.fileSize;
            failedArchiveReq += rsr.copyNbsToRearchive.size();
            ++subReqItor;
            ++nbSubReqProcessed;
            continue;
          }
          rr.setActiveCopyNumber(activeCopyNumber);
          m_lc.log(log::DEBUG, "In RepackRequest::addSubrequestsAndUpdateStats(): about to emplace the row to vector.");
          if (rsr.hasUserProvidedFile) {
            rr.setJobStatus(activeCopyNumber, RetrieveJobStatus::RJS_ToReportToRepackForSuccess);
            rrRowBatchNoRecall.emplace_back(rr.makeJobRow());
            log::ScopedParamContainer(m_lc)
              .add("rearchiveCopyNbs", rrRowBatchNoRecall.back()->rearchiveCopyNbs)
              .log(log::DEBUG,
                   "In RepackRequest::addSubrequestsAndUpdateStats(): rrRowBatchNoRecall.back().rearchiveCopyNbs.");
          } else {
            rr.setJobStatus(activeCopyNumber, RetrieveJobStatus::RJS_ToTransfer);
            rrRowBatchToTransfer.emplace_back(rr.makeJobRow());
            log::ScopedParamContainer(m_lc)
              .add("rearchiveCopyNbs back", rrRowBatchToTransfer.back()->rearchiveCopyNbs)
              .log(log::DEBUG,
                   "In RepackRequest::addSubrequestsAndUpdateStats(): rrRowBatchToTransfer->back().rearchiveCopyNbs.");
          }
        } catch (exception::Exception& ex) {
          failedCreationStats.files++;
          failedCreationStats.bytes += rsr.archiveFile.fileSize;
          failedArchiveReq += rsr.copyNbsToRearchive.size();
          cta::log::ScopedParamContainer params(m_lc);
          params.add("exceptionMessage", ex.getMessageValue());
          m_lc.log(log::ERR, "Failed to create subrequest.");
        }
      }
      ++subReqItor;
      ++nbSubReqProcessed;
    }
    m_lc.log(log::DEBUG, "In RepackRequest::addSubrequestsAndUpdateStats(): about to insert bunch to the DB.");

    // --- Insert ToTransfer jobs ---
    if (!rrRowBatchToTransfer.empty()) {
      log::ScopedParamContainer params(m_lc);
      params.add("nrows", rrRowBatchToTransfer.size());
      auto conn = m_connPool.getConn();
      try {
        cta::schedulerdb::postgres::RetrieveJobQueueRow::insertBatch(conn, rrRowBatchToTransfer, true);
        nbRetrieveSubrequestsCreated += rrRowBatchToTransfer.size();
        m_lc.log(log::INFO,
                 "In RepackRequest::addSubrequestsAndUpdateStats(): inserted bunch of 'ToTransfer' retrieve jobs.");
      } catch (exception::Exception& ex) {
        params.add("exceptionMessage", ex.getMessageValue());
        m_lc.log(log::ERR,
                 "In RepackRequest::addSubrequestsAndUpdateStats(): failed to insert 'ToTransfer' retrieve jobs.");
        conn.rollback();
        // all these failed rows should be counted as not created
        for (auto& row : rrRowBatchToTransfer) {
          failedCreationStats.files++;
          failedCreationStats.bytes += row->fileSize;
          failedArchiveReq += srmap[row->fSeq]->archiveCopyNbsSet.size();
        }
      }
    }

    // --- Insert NoRecall jobs ---
    if (!rrRowBatchNoRecall.empty()) {
      log::ScopedParamContainer params(m_lc);
      params.add("nrows", rrRowBatchNoRecall.size());
      auto conn = m_connPool.getConn();
      try {
        cta::schedulerdb::postgres::RetrieveJobQueueRow::insertBatch(conn, rrRowBatchNoRecall, true);
        nbRetrieveSubrequestsCreated += rrRowBatchNoRecall.size();
        m_lc.log(log::INFO,
                 "In RepackRequest::addSubrequestsAndUpdateStats(): inserted bunch of 'NoRecall' retrieve jobs.");
      } catch (exception::Exception& ex) {
        params.add("exceptionMessage", ex.getMessageValue());
        m_lc.log(log::ERR,
                 "In RepackRequest::addSubrequestsAndUpdateStats(): failed to insert 'NoRecall' retrieve jobs.");
        conn.rollback();
        for (auto& row : rrRowBatchNoRecall) {
          failedCreationStats.files++;
          failedCreationStats.bytes += row->fileSize;
          failedArchiveReq += srmap[row->fSeq]->archiveCopyNbsSet.size();
        }
      }
    }

    if (failedArchiveReq != 0 || failedCreationStats.files != 0) {
      reportRetrieveCreationFailures(failedCreationStats.files, failedCreationStats.bytes, failedArchiveReq);
      failedArchiveReq = 0;
      failedCreationStats = StatsValues {};
    }
  }
  setLastExpandedFSeq(fSeq);
  cta::schedulerdb::Transaction txn(m_connPool, m_lc);
  try {
    uint64_t nrows =
      postgres::RepackRequestTrackingRow::updateRepackRequest(txn,
                                                              repackInfo.repackReqId,
                                                              totalStatsFiles,
                                                              nbRetrieveSubrequestsCreated,
                                                              fSeq,
                                                              mapRepackInfoStatusToJobStatus(repackInfo.status));
    log::ScopedParamContainer(m_lc)
      .add("nrows", nrows)
      .log(
        log::INFO,
        "In RepackRequest::addSubrequestsAndUpdateStats(): updateRepackRequest() called successfully after expansion.");
    txn.commit();
  } catch (cta::exception::Exception& e) {
    std::string bt = e.backtrace();
    m_lc.log(log::ERR,
             "In RepackRequest::addSubrequestsAndUpdateStats(): updateRepackRequest() Exception thrown: " + bt);
    txn.abort();
  }
  return nbRetrieveSubrequestsCreated;
}

void RepackRequest::expandDone() {
  repackInfo.isExpandFinished = true;
  setExpandStartedAndChangeStatus();
}

void RepackRequest::fail() {
  cta::schedulerdb::Transaction txn(m_connPool, m_lc);
  try {
    repackInfo.status = common::dataStructures::RepackInfo::Status::Failed;
    uint64_t nrows = postgres::RepackRequestTrackingRow::updateRepackRequestStatusAndFinishTime(
      txn,
      repackInfo.repackReqId,
      repackInfo.isExpandFinished,
      RepackJobStatus::RRS_Failed,
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

    log::ScopedParamContainer(m_lc)
      .add("nrows", nrows)
      .add("newStatus", to_string(RepackJobStatus::RRS_Failed))
      .log(log::INFO, "In RepackRequest::fail(): marked repack request as failed.");
    txn.commit();
  } catch (cta::exception::Exception& e) {
    log::ScopedParamContainer(m_lc)
      .add("newStatus", to_string(RepackJobStatus::RRS_Failed))
      .add("repackInfo.repackReqId", repackInfo.repackReqId)
      .add("repackInfo.repackFinishedTime", repackInfo.repackFinishedTime)
      .log(log::ERR, "Exception updating status: " + e.backtrace());
    txn.abort();
  }
}

void RepackRequest::requeueInToExpandQueue(log::LogContext& lc) {
  throw cta::exception::NotImplementedException();
}

void RepackRequest::setExpandStartedAndChangeStatus() {
  if (!repackInfo.isExpandStarted) {
    throw exception::Exception("In RepackRequest::setExpandStartedAndChangeStatus(): "
                               "should not attempt to run, IS_EXPAND_STARTED is false.");
  }

  // Evaluate new status
  auto newStatus = getCurrentStatus();
  repackInfo.status = newStatus;

  cta::schedulerdb::Transaction txn(m_connPool, m_lc);

  try {
    if (newStatus == common::dataStructures::RepackInfo::Status::Complete
        || newStatus == common::dataStructures::RepackInfo::Status::Failed) {
      repackInfo.repackFinishedTime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      log::ScopedParamContainer(m_lc)
        .add("newStatus", to_string(mapRepackInfoStatusToJobStatus(newStatus)))
        .log(
          log::DEBUG,
          "RepackRequest::setExpandStartedAndChangeStatus(): updateRepackRequestStatusAndFinishTime() before update");

      uint64_t nrows = postgres::RepackRequestTrackingRow::updateRepackRequestStatusAndFinishTime(
        txn,
        repackInfo.repackReqId,
        repackInfo.isExpandFinished,
        mapRepackInfoStatusToJobStatus(newStatus),
        repackInfo.repackFinishedTime);

      log::ScopedParamContainer(m_lc)
        .add("nrows", nrows)
        .add("newStatus", to_string(mapRepackInfoStatusToJobStatus(newStatus)))
        .log(log::INFO, "updateRepackRequestStatusAndFinishTime finished");
    } else {
      uint64_t nrows =
        postgres::RepackRequestTrackingRow::updateRepackRequestStatus(txn,
                                                                      repackInfo.repackReqId,
                                                                      repackInfo.isExpandFinished,
                                                                      mapRepackInfoStatusToJobStatus(newStatus));

      log::ScopedParamContainer(m_lc).add("nrows", nrows).log(log::INFO, "updateRepackRequestStatus finished");
    }
    txn.commit();
  } catch (cta::exception::Exception& e) {
    log::ScopedParamContainer(m_lc)
      .add("newStatus", to_string(mapRepackInfoStatusToJobStatus(newStatus)))
      .add("repackInfo.repackReqId", repackInfo.repackReqId)
      .add("repackInfo.isExpandFinished", repackInfo.isExpandFinished)
      .add("repackInfo.repackFinishedTime", repackInfo.repackFinishedTime)
      .log(log::ERR, "Exception updating status: " + e.backtrace());
    txn.abort();
  }
}

common::dataStructures::RepackInfo::Status RepackRequest::getCurrentStatus() const {
  {
    bool finishedExpansion = repackInfo.isExpandFinished;
    bool allRetrieveDone =
      (repackInfo.retrievedFiles + repackInfo.failedFilesToRetrieve) >= repackInfo.totalFilesToRetrieve;
    bool allArchiveDone = (repackInfo.archivedFiles + repackInfo.failedFilesToArchive + m_failedToCreateArchiveReq)
                          >= repackInfo.totalFilesToArchive;
    if (finishedExpansion && allRetrieveDone && allArchiveDone) {
      if (repackInfo.failedFilesToRetrieve > 0 || repackInfo.failedFilesToArchive > 0) {
        return common::dataStructures::RepackInfo::Status::Failed;
      } else {
        return common::dataStructures::RepackInfo::Status::Complete;
      }
    }
  }
  if (repackInfo.retrievedFiles > 0 || repackInfo.failedFilesToRetrieve > 0 || repackInfo.archivedFiles > 0
      || repackInfo.failedFilesToArchive > 0) {
    return common::dataStructures::RepackInfo::Status::Running;
  }

  return common::dataStructures::RepackInfo::Status::Starting;
}

void RepackRequest::fillLastExpandedFSeqAndTotalStatsFile(uint64_t& fSeq, TotalStatsFiles& totalStatsFiles) {
  fSeq = repackInfo.lastExpandedFseq;
  totalStatsFiles = getTotalStatsFile();
}

//------------------------------------------------------------------------------
// RepackRequest::getTotalStatsFile()
//------------------------------------------------------------------------------
cta::SchedulerDatabase::RepackRequest::TotalStatsFiles RepackRequest::getTotalStatsFile() {
  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles ret;
  ret.totalFilesOnTapeAtStart = repackInfo.totalFilesOnTapeAtStart;
  ret.totalBytesOnTapeAtStart = repackInfo.totalBytesOnTapeAtStart;
  ret.allFilesSelectedAtStart = repackInfo.allFilesSelectedAtStart;
  ret.totalBytesToRetrieve = repackInfo.totalBytesToRetrieve;
  ret.totalBytesToArchive = repackInfo.totalBytesToArchive;
  ret.totalFilesToRetrieve = repackInfo.totalFilesToRetrieve;
  ret.totalFilesToArchive = repackInfo.totalFilesToArchive;
  ret.userProvidedFiles = repackInfo.userProvidedFiles;
  return ret;
}

void RepackRequest::setVid(const std::string& vid) {
  repackInfo.vid = vid;
}

void RepackRequest::setType(common::dataStructures::RepackInfo::Type repackType) {
  using RepackType = common::dataStructures::RepackInfo::Type;
  switch (repackType) {
    case RepackType::MoveAndAddCopies:
      // Nothing to do, this is the default case.
      break;
    case RepackType::AddCopiesOnly:
      m_isMove = false;
      break;
    case RepackType::MoveOnly:
      m_addCopies = false;
      break;
    default:
      throw exception::Exception("In RepackRequest::setType(): unexpected type.");
  }
}

void RepackRequest::setBufferURL(const std::string& bufferURL) {
  repackInfo.repackBufferBaseURL = bufferURL;
}

void RepackRequest::setMountPolicy(const common::dataStructures::MountPolicy& mp) {
  m_mountPolicy = mp;
}

void RepackRequest::setNoRecall(const bool noRecall) {
  m_noRecall = noRecall;
}

void RepackRequest::setCreationLog(const common::dataStructures::EntryLog& creationLog) {
  m_creationLog = creationLog;
}

void RepackRequest::setMaxFilesToSelect(const uint64_t maxFilesToSelect) {
  m_maxFilesToSelect = maxFilesToSelect;
}

void RepackRequest::insert() {
  log::ScopedParamContainer params(m_lc);

  try {
    cta::schedulerdb::postgres::RepackRequestTrackingRow rjr;

    rjr.vid = repackInfo.vid;
    rjr.bufferUrl = repackInfo.repackBufferBaseURL;
    rjr.mountPolicyName = m_mountPolicy.name;
    rjr.isNoRecall = m_noRecall;
    rjr.createLog = m_creationLog;
    rjr.isMove = m_isMove;
    rjr.isAddCopies = m_addCopies;
    rjr.maxFilesToSelect = m_maxFilesToSelect;

    if (repackInfo.destinationInfos.size() > 0 || m_subreqp.size() > 0) {
      throw cta::exception::Exception("RepackRequest::insert expected zero subreqs and desintionInfos in new request");
    }

    rjr.addParamsToLogContext(params);
    m_lc.log(log::INFO, "In RepackRequest::insert(): before inserting row");
    auto conn = m_connPool.getConn();
    rjr.insert(conn);
  } catch (exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In RepackRequest::insert(): failed to queue request.");
    throw;
  }
  m_lc.log(log::INFO, "In RepackRequest::insert(): added request to queue.");
}

void RepackRequest::assignJobStatusToRepackInfoStatus(const RepackJobStatus& dbStatus) {
  switch (dbStatus) {
    case RepackJobStatus::RRS_Pending:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Pending;
      break;
    case RepackJobStatus::RRS_ToExpand:
      repackInfo.status = common::dataStructures::RepackInfo::Status::ToExpand;
      break;
    case RepackJobStatus::RRS_Starting:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Starting;
      break;
    case RepackJobStatus::RRS_Running:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Running;
      break;
    case RepackJobStatus::RRS_Complete:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Complete;
      break;
    case RepackJobStatus::RRS_Failed:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Failed;
      break;
    default:
      repackInfo.status = common::dataStructures::RepackInfo::Status::Undefined;
      break;
  }
}

RepackJobStatus
RepackRequest::mapRepackInfoStatusToJobStatus(const common::dataStructures::RepackInfo::Status& infoStatus) {
  switch (infoStatus) {
    case common::dataStructures::RepackInfo::Status::Pending:
      return RepackJobStatus::RRS_Pending;
    case common::dataStructures::RepackInfo::Status::ToExpand:
      return RepackJobStatus::RRS_ToExpand;
    case common::dataStructures::RepackInfo::Status::Starting:
      return RepackJobStatus::RRS_Starting;
    case common::dataStructures::RepackInfo::Status::Running:
      return RepackJobStatus::RRS_Running;
    case common::dataStructures::RepackInfo::Status::Complete:
      return RepackJobStatus::RRS_Complete;
    case common::dataStructures::RepackInfo::Status::Failed:
      return RepackJobStatus::RRS_Failed;
    default:
      throw cta::exception::Exception("In RepackRequest::mapRepackInfoStatusToJobStatus(): unknown RepackInfo Status.");
  }
}

RepackRequest& RepackRequest::operator=(const schedulerdb::postgres::RepackRequestTrackingRow& row) {
  m_failedToCreateArchiveReq = row.failedToCreateArchiveReq;
  repackInfo.repackReqId = row.repackReqId;
  repackInfo.mountPolicy = row.mountPolicyName;
  repackInfo.vid = row.vid;
  repackInfo.repackBufferBaseURL = row.bufferUrl;
  repackInfo.type = common::dataStructures::RepackInfo::Type::Undefined;
  if (row.isMove) {
    if (row.isAddCopies) {
      repackInfo.type = common::dataStructures::RepackInfo::Type::MoveAndAddCopies;
    } else {
      repackInfo.type = common::dataStructures::RepackInfo::Type::MoveOnly;
    }
  } else if (row.isAddCopies) {
    repackInfo.type = common::dataStructures::RepackInfo::Type::AddCopiesOnly;
  }
  assignJobStatusToRepackInfoStatus(row.status);

  repackInfo.totalFilesOnTapeAtStart = row.totalFilesOnTapeAtStart;
  repackInfo.totalBytesOnTapeAtStart = row.totalBytesOnTapeAtStart;
  repackInfo.allFilesSelectedAtStart = row.allFilesSelectedAtStart;
  repackInfo.totalFilesToArchive = row.totalFilesToArchive;
  repackInfo.totalBytesToArchive = row.totalBytesToArchive;
  repackInfo.totalFilesToRetrieve = row.totalFilesToRetrieve;
  repackInfo.totalBytesToRetrieve = row.totalBytesToRetrieve;
  repackInfo.failedFilesToArchive = row.failedToArchiveFiles;
  repackInfo.failedBytesToArchive = row.failedToArchiveBytes;
  repackInfo.failedFilesToRetrieve = row.failedToRetrieveFiles;
  repackInfo.failedBytesToRetrieve = row.failedToRetrieveBytes;
  repackInfo.lastExpandedFseq = row.lastExpandedFseq;
  repackInfo.userProvidedFiles = row.userProvidedFiles;
  repackInfo.retrievedFiles = row.retrievedFiles;
  repackInfo.retrievedBytes = row.retrievedBytes;
  repackInfo.archivedFiles = row.archivedFiles;
  repackInfo.archivedBytes = row.archivedBytes;
  repackInfo.isExpandStarted = row.isExpandStarted;
  repackInfo.isExpandFinished = row.isExpandFinished;
  repackInfo.noRecall = row.isNoRecall;
  repackInfo.creationLog = row.createLog;
  repackInfo.repackFinishedTime = row.repackFinishedTime;
  repackInfo.maxFilesToSelect = row.maxFilesToSelect;
  // The repackInfo.destinationInfos are not filled here, but rather separately
  // when getRepackInfo is requested explicitly
  // This could be improved by refactoring.
  m_subreqp.clear();

  // m_mountPolicy
  m_mountPolicy.name = row.mountPolicyName;

  m_noRecall = row.isNoRecall;
  common::dataStructures::EntryLog m_creationLog;
  m_addCopies = row.isAddCopies;
  m_isMove = row.isMove;
  m_isComplete = row.isComplete;
  m_maxFilesToSelect = row.maxFilesToSelect;
  m_failedToCreateArchiveReq = row.failedToCreateArchiveReq;
  return *this;
}

}  // namespace cta::schedulerdb
