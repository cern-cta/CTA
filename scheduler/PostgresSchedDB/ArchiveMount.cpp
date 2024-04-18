/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "scheduler/PostgresSchedDB/ArchiveMount.hpp"
#include "scheduler/PostgresSchedDB/ArchiveJob.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/PostgresSchedDB/sql/ArchiveJobQueue.hpp"
#include "scheduler/PostgresSchedDB/sql/Transaction.hpp"
#include "catalogue/TapeDrivesCatalogueState.hpp"


namespace cta::postgresscheddb {

const SchedulerDatabase::ArchiveMount::MountInfo &ArchiveMount::getMountInfo()
{
  return mountInfo;
}

std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ArchiveMount::getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext& logContext)
{
  logContext.log(cta::log::DEBUG, "Entering ArchiveMount::getNextJobBatch()");
  rdbms::Rset resultSet;
  // fetch a non transactional connection from the PGSCHED connection pool
  auto conn = m_PostgresSchedDB.m_connPool.getConn();
  // retrieve batch up to file limit
  if(m_queueType == common::dataStructures::JobQueueType::JobsToTransferForUser) {
    logContext.log(cta::log::DEBUG, "Query JobsToTransferForUser ArchiveMount::getNextJobBatch()");
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            conn, ArchiveJobStatus::AJS_ToTransferForUser, mountInfo.tapePool, filesRequested);
    logContext.log(cta::log::DEBUG, "After first selection of AJS_ToTransferForUser ArchiveMount::getNextJobBatch()");
  } else {
    logContext.log(cta::log::DEBUG, "Query ArchiveJobQueueRow ArchiveMount::getNextJobBatch()");
    resultSet = cta::postgresscheddb::sql::ArchiveJobQueueRow::select(
            conn, ArchiveJobStatus::AJS_ToTransferForRepack, mountInfo.tapePool, filesRequested);
  }
  logContext.log(cta::log::DEBUG, "Filling jobs in ArchiveMount::getNextJobBatch()");
  std::list<sql::ArchiveJobQueueRow> jobs;
  std::string jobIDsString;
  // filter retrieved batch up to size limit
  uint64_t totalBytes = 0;
  while(resultSet.next()) {
    logContext.log(cta::log::DEBUG, "I see a selected job in the resultSet");
    uint64_t myjobid =  resultSet.columnUint64("JOB_ID");
    jobs.emplace_back(resultSet);
    jobIDsString += std::to_string(myjobid) + ",";
    logContext.log(cta::log::DEBUG, "jobIDsString: " + jobIDsString);
    totalBytes += jobs.back().archiveFile.fileSize;
    if(totalBytes >= bytesRequested) break;
  }
  logContext.log(cta::log::DEBUG, "Ended filling jobs in ArchiveMount::getNextJobBatch() executing ArchiveJobQueueRow::updateMountID");
  if (!jobIDsString.empty()) {
    jobIDsString.pop_back(); // Remove the trailing comma
    // this shall be condition for the update otherwise we do not need to get any connection
    // we shall move the condition here rather than relying on updateMountID to return doing nothing
    logContext.log(cta::log::DEBUG, "JOBIDS string looks like this: " + jobIDsString + "END and mountID line this:" +
                                    std::to_string(mountInfo.mountId) + "END");
  }
  // mark the jobs in the batch as owned
  try {
    cta::postgresscheddb::Transaction txn(m_PostgresSchedDB.m_connPool);
    txn.lockGlobal(0);
    sql::ArchiveJobQueueRow::updateMountID(txn, jobIDsString, mountInfo.mountId);
    txn.commit();
  } catch (exception::Exception& ex) {
    logContext.log(cta::log::DEBUG, "In sql::ArchiveJobQueueRow::updateMountID: failed to update MOunt ID." + ex.getMessageValue());
  }
  logContext.log(cta::log::DEBUG, "Finished updating Mount ID for the selected jobs  ArchiveJobQueueRow::updateMountID" + jobIDsString);

  // Construct the return value
  std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> ret;
  for (const auto &j : jobs) {
    auto aj = std::make_unique<postgresscheddb::ArchiveJob>(/* j.jobId */);
    aj->tapeFile.copyNb = j.copyNb;
    aj->archiveFile = j.archiveFile;
    aj->archiveReportURL = j.archiveReportUrl;
    aj->errorReportURL = j.archiveErrorReportUrl;
    aj->srcURL = j.srcUrl;
    aj->tapeFile.fSeq = ++nbFilesCurrentlyOnTape;
    aj->tapeFile.vid = mountInfo.vid;
    aj->tapeFile.blockId = std::numeric_limits<decltype(aj->tapeFile.blockId)>::max();
// m_jobOwned ?
    aj->m_mountId = mountInfo.mountId;
    aj->m_tapePool = mountInfo.tapePool;
// reportType ?
    ret.emplace_back(std::move(aj));
  }
  return ret;
}

void ArchiveMount::setDriveStatus(common::dataStructures::DriveStatus status, common::dataStructures::MountType mountType,
                                time_t completionTime, const std::optional<std::string>& reason)
{
  // We just report the drive status as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host = mountInfo.host;
  ReportDriveStatusInputs inputs;
  inputs.mountType = mountType;
  inputs.mountSessionId = mountInfo.mountId;
  inputs.reportTime = completionTime;
  inputs.status = status;
  inputs.vid = mountInfo.vid;
  inputs.tapepool = mountInfo.tapePool;
  inputs.vo = mountInfo.vo;
  inputs.reason = reason;
  // TODO: statistics!
  inputs.byteTransferred = 0;
  inputs.filesTransferred = 0;
  log::LogContext lc(m_PostgresSchedDB.m_logger);
  m_PostgresSchedDB.m_tapeDrivesState->updateDriveStatus(driveInfo, inputs, lc);
}

void ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats)
{
  // We just report the tape session statistics as instructed by the tape thread.
  // Reset the drive state.
  common::dataStructures::DriveInfo driveInfo;
  driveInfo.driveName = mountInfo.drive;
  driveInfo.logicalLibrary = mountInfo.logicalLibrary;
  driveInfo.host=mountInfo.host;

  ReportDriveStatsInputs inputs;
  inputs.reportTime = time(nullptr);
  inputs.bytesTransferred = stats.dataVolume;
  inputs.filesTransferred = stats.filesCount;

  log::LogContext lc(m_PostgresSchedDB.m_logger);
  m_PostgresSchedDB.m_tapeDrivesState->updateDriveStatistics(driveInfo, inputs, lc);
}

void ArchiveMount::setJobBatchTransferred(
      std::list<std::unique_ptr<SchedulerDatabase::ArchiveJob>> & jobsBatch, log::LogContext & lc)
{
  std::set<cta::postgresscheddb::ArchiveJob*> jobsToQueueForReportingToUser, jobsToQueueForReportingToRepack, failedJobsToQueueForReportingForRepack;
  std::unordered_map<std::string, std::vector<uint64_t>> ajToUnown;

  utils::Timer t;
  log::TimingList timingList;
  // We will asynchronously report the archive jobs (which MUST be postgresscheddb Jobs).
  // We let the exceptions through as failing to report is fatal.
  auto jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    log::ScopedParamContainer(lc)
            .add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
            .add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
            .add("archiveFileID", std::to_string(castFromSchedDBJob(jobsBatchItor->get())->archiveFile.archiveFileID))
            .add("diskInstance", castFromSchedDBJob(jobsBatchItor->get())->archiveFile.diskInstance)
            .log(log::INFO,
                 "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): received a job to be reported.");
    try {
      castFromSchedDBJob(jobsBatchItor->get())->asyncSucceedTransfer();
      jobsBatchItor++;
    } catch (cta::exception::NoSuchObject& ex) {
      jobsBatch.erase(jobsBatchItor++);
      log::ScopedParamContainer(lc)
              .add("tapeVid", (*jobsBatchItor)->tapeFile.vid)
              .add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
              .add("archiveFileID", std::to_string(castFromSchedDBJob(jobsBatchItor->get())->archiveFile.archiveFileID))
              .add("diskInstance", castFromSchedDBJob(jobsBatchItor->get())->archiveFile.diskInstance)
              .add("exceptionMessage", ex.getMessageValue())
              .log(log::WARNING,
                   "In postgresscheddb::RetrieveMount::setJobBatchTransferred(): async succeed transfer failed, "
                   "job does not exist in the Scheduler DB.");
    }
  }

  timingList.insertAndReset("asyncSucceedLaunchTime", t);
  // We will only know whether we need to queue the requests for user for reporting after updating request. So on a first
  // pass we update the request and on the second, we will queue a batch of them to the report queue. Report queues
  // are per VID and not tape pool: this limits contention (one tape written to at a time per mount, so the queuing should
  // be without contention.
  // Jobs that do not require queuing are done from our perspective and we should just remove them from agent ownership.
  // Jobs for repack always get reported.
  jobsBatchItor = jobsBatch.begin();
  while (jobsBatchItor != jobsBatch.end()) {
    try {
      castFromSchedDBJob(jobsBatchItor->get())->waitAsyncSucceed();
      auto repackInfo = castFromSchedDBJob(jobsBatchItor->get())->getRepackInfoAfterAsyncSuccess();
      if (repackInfo.isRepack) {
        jobsToQueueForReportingToRepack.insert(castFromSchedDBJob(jobsBatchItor->get()));
      } else {
        if (castFromSchedDBJob(jobsBatchItor->get())->isLastAfterAsyncSuccess())
          jobsToQueueForReportingToUser.insert(castFromSchedDBJob(jobsBatchItor->get()));
        else
          ajToUnown[castFromSchedDBJob(jobsBatchItor->get())->archiveFile.diskInstance].push_back(castFromSchedDBJob(jobsBatchItor->get())->archiveFile.archiveFileID);
      }
      jobsBatchItor++;
    } catch (cta::exception::NoSuchObject& ex) {
      jobsBatch.erase(jobsBatchItor++);
      log::ScopedParamContainer(lc)
              .add("fileId", (*jobsBatchItor)->archiveFile.archiveFileID)
              .add("exceptionMessage", ex.getMessageValue())
              .log(log::WARNING,
                   "In postgresscheddb::RetrieveMount::setJobBatchTransferred(): wait async succeed transfer failed, "
                   "job does not exist in the Scheduler DB.");
    }
  }
  timingList.insertAndReset("asyncSucceedCompletionTime", t);
  if (jobsToQueueForReportingToUser.size()) {
    typedef objectstore::ContainerAlgorithms<objectstore::ArchiveQueue,objectstore::ArchiveQueueToReportForUser> AqtrCa;
    AqtrCa aqtrCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    std::map<std::string, AqtrCa::InsertedElement::list> insertedElementsLists;
    for (auto& j : jobsToQueueForReportingToUser) {
      insertedElementsLists[j->tapeFile.vid].emplace_back(AqtrCa::InsertedElement{&j->m_archiveRequest, j->tapeFile.copyNb,
                                                                                  j->archiveFile, std::nullopt, std::nullopt});
      log::ScopedParamContainer(lc)
              .add("tapeVid", j->tapeFile.vid)
              .add("fileId", j->archiveFile.archiveFileID)
              .add("requestObject", j->m_archiveRequest.getAddressIfSet())
              .log(log::INFO,
                   "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): will queue request for reporting to user.");
    }
    for (auto& list : insertedElementsLists) {
      try {
        utils::Timer tLocal;
        aqtrCa.referenceAndSwitchOwnership(list.first, m_oStoreDB.m_agentReference->getAgentAddress(), list.second, lc);
        log::ScopedParamContainer(lc)
                .add("tapeVid", list.first)
                .add("jobs", list.second.size())
                .add("enqueueTime", t.secs())
                .log(log::INFO,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): queued a batch of requests for "
                     "reporting to user.");
      } catch (cta::exception::Exception& ex) {
        log::ScopedParamContainer(lc)
                .add("tapeVid", list.first)
                .add("exceptionMSG", ex.getMessageValue())
                .log(log::ERR,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests "
                     "for reporting to user.");
        lc.logBacktrace(log::INFO, ex.backtrace());
      }
    }
    timingList.insertAndReset("queueingToReportToUserTime", t);
  }
  if (jobsToQueueForReportingToRepack.size()) {
    typedef objectstore::ContainerAlgorithms<objectstore::ArchiveQueue,objectstore::ArchiveQueueToReportToRepackForSuccess> AqtrtrCa;
    AqtrtrCa aqtrtrCa(m_oStoreDB.m_objectStore, *m_oStoreDB.m_agentReference);
    std::map<std::string, AqtrtrCa::InsertedElement::list> insertedElementsLists;
    for (auto& j : jobsToQueueForReportingToRepack) {
      insertedElementsLists[j->getRepackInfoAfterAsyncSuccess().repackRequestAddress].emplace_back(AqtrtrCa::InsertedElement{&j->m_archiveRequest, j->tapeFile.copyNb,
                                                                                                                             j->archiveFile, std::nullopt, std::nullopt});
      log::ScopedParamContainer(lc)
              .add("repackRequestAddress", j->getRepackInfoAfterAsyncSuccess().repackRequestAddress)
              .add("fileId", j->archiveFile.archiveFileID)
              .add("requestObject", j->m_archiveRequest.getAddressIfSet())
              .log(log::INFO,
                   "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): will queue request for reporting "
                   "to repack.");
    }
    for (auto& list : insertedElementsLists) {
      int currentTotalRetries = 0;
      int maxRetries = 10;
      retry:
      try {
        utils::Timer tLocal;
        aqtrtrCa.referenceAndSwitchOwnership(list.first, m_oStoreDB.m_agentReference->getAgentAddress(), list.second, lc);
        log::ScopedParamContainer(lc)
                .add("repackRequestAddress", list.first)
                .add("jobs", list.second.size())
                .add("enqueueTime", t.secs())
                .log(log::INFO,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): queued a batch of requests for "
                     "reporting to repack.");
      } catch (cta::exception::NoSuchObject& ex) {
        log::ScopedParamContainer(lc)
                .add("tapeVid", list.first)
                .add("exceptionMSG", ex.getMessageValue())
                .log(log::WARNING,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for "
                     "reporting to repack, jobs do not exist in the Scheduler DB.");
      } catch (const AqtrtrCa::OwnershipSwitchFailure& ex) {
        //We are in the case where the ownership of the elements could not have been change (most probably because of a Rados lockbackoff error)
        //We will then retry 10 times to requeue the failed jobs
        log::ScopedParamContainer(lc)
                .add("tapeVid", list.first)
                .add("numberOfRetries", currentTotalRetries)
                .add("numberOfFailedToQueueElements", ex.failedElements.size())
                .add("exceptionMSG", ex.getMessageValue())
                .log(log::WARNING,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): unable to queue some elements "
                     "because of an ownership switch failure, retrying another time");
        typedef objectstore::ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>::OpFailure<AqtrtrCa::InsertedElement> OpFailure;
        list.second.remove_if([&ex](const AqtrtrCa::InsertedElement& elt) {
          //Remove the elements that are NOT in the failed elements list so that we only retry the failed elements
          return std::find_if(ex.failedElements.begin(),ex.failedElements.end(),[&elt](const OpFailure& insertedElement){
            return elt.archiveRequest->getAddressIfSet() == insertedElement.element->archiveRequest->getAddressIfSet() && elt.copyNb == insertedElement.element->copyNb;
          }) == ex.failedElements.end();
        });
        currentTotalRetries++;
        if(currentTotalRetries <= maxRetries){
          goto retry;
        } else {
          //All the retries have been done, throw an exception for logging the backtrace
          //afterwards
          throw cta::exception::Exception(ex.getMessageValue());
        }
      } catch (cta::exception::Exception& ex) {
        log::ScopedParamContainer(lc)
                .add("tapeVid", list.first)
                .add("exceptionMSG", ex.getMessageValue())
                .log(log::ERR,
                     "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): failed to queue a batch of requests for "
                     "reporting to repack.");
        lc.logBacktrace(log::INFO, ex.backtrace());
      }
    }
    timingList.insertAndReset("queueingToReportToRepackTime", t);
  }
  // sort list by instance name
  ajToUnown.sort([](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  if (ajToUnown.size()) {
    m_oStoreDB.m_agentReference->removeBatchFromOwnership(ajToUnown, m_oStoreDB.m_objectStore);
    timingList.insertAndReset("removeFromOwnershipTime", t);
  }

  log::ScopedParamContainer params(lc);
  params.add("QueuedRequests", jobsToQueueForReportingToUser.size())
          .add("PartiallyCompleteRequests", ajToUnown.size());
  timingList.addToLog(params);
  lc.log(log::INFO,
         "In postgresscheddb::ArchiveMount::setJobBatchTransferred(): set ArchiveRequests successful and "
         "queued for reporting.");
}

} // namespace cta::postgresscheddb
