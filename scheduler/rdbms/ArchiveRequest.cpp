/**
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright © 2021-2022 CERN
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

#include "scheduler/rdbms/ArchiveRequest.hpp"
#include "scheduler/rdbms/postgres/ArchiveJobQueue.hpp"
#include "common/Timer.hpp"

namespace cta::schedulerdb {

[[noreturn]] void ArchiveRequest::update() const {
  throw std::runtime_error("update not implemented.");
}

postgres::ArchiveJobQueueRow
ArchiveRequest::makeJobRow(const Job& archiveJob) const {

  postgres::ArchiveJobQueueRow ajr;
  // Setting common fields
  if (m_jobs.size() == 1) {
      ajr.diskFileId           = std::move(m_archiveFile.diskFileId);
      ajr.diskInstance         = std::move(m_archiveFile.diskInstance);
      ajr.storageClass         = std::move(m_archiveFile.storageClass);
      ajr.diskFileInfoPath     = std::move(m_archiveFile.diskFileInfo.path);
      ajr.checksumBlob         = std::move(m_archiveFile.checksumBlob);
      ajr.archiveReportURL     = std::move(m_archiveReportURL);
      ajr.archiveErrorReportURL= std::move(m_archiveErrorReportURL);
      ajr.srcUrl               = std::move(m_srcURL);
    } else {
      ajr.diskFileId           = m_archiveFile.diskFileId;
      ajr.diskInstance         = m_archiveFile.diskInstance;
      ajr.storageClass         = m_archiveFile.storageClass;
      ajr.diskFileInfoPath     = m_archiveFile.diskFileInfo.path;
      ajr.checksumBlob         = m_archiveFile.checksumBlob;
      ajr.archiveReportURL     = m_archiveReportURL;
      ajr.archiveErrorReportURL= m_archiveErrorReportURL;
      ajr.srcUrl               = m_srcURL;
    }

  ajr.reqId          = cta::schedulerdb::postgres::ArchiveJobQueueRow::getNextArchiveRequestID(m_conn);
  ajr.reqJobCount    = areq_job_count;
  ajr.mountPolicy    = m_mountPolicy.name;
  ajr.priority       = m_mountPolicy.archivePriority;
  ajr.minArchiveRequestAge = m_mountPolicy.archiveMinRequestAge;

  ajr.archiveFileID  = m_archiveFile.archiveFileID;
  ajr.fileSize       = m_archiveFile.fileSize;
  ajr.diskFileInfoOwnerUid = m_archiveFile.diskFileInfo.owner_uid;
  ajr.diskFileInfoGid      = m_archiveFile.diskFileInfo.gid;

  ajr.creationTime   = m_entryLog.time; // Time received by frontend
  ajr.startTime      = time(nullptr);   // Time queued in DB
  ajr.requesterName  = m_requesterIdentity.name;
  ajr.requesterGroup = m_requesterIdentity.group;
  // Setting values unique for each job from m_jobs
  ajr.copyNb         = archiveJob.copyNb;
  ajr.tapePool       = archiveJob.tapepool;
  ajr.retriesWithinMount   = archiveJob.retriesWithinMount;
  ajr.maxRetriesWithinMount= archiveJob.maxRetriesWithinMount;
  ajr.maxReportRetries     = archiveJob.maxReportRetries;
  ajr.totalRetries         = archiveJob.totalRetries;
  ajr.lastMountWithFailure = archiveJob.lastMountWithFailure;
  ajr.maxTotalRetries      = archiveJob.maxTotalRetries;

  return ajr;
}

// Inserts one row into DB
void ArchiveRequest::insert() {
  try{
    if (m_jobs.size() == 1) {
      const auto& aj = m_jobs.front();
      postgres::ArchiveJobQueueRow row = makeJobRow(aj);
      row.addParamsToLogContext(params);
      row.insert(m_conn);
      m_lc.log(log::INFO, "In ArchiveRequest::insert(): added job to queue.");
    } else {
      std::vector<postgres::ArchiveJobQueueRow> rows;
      rows.reserve(m_jobs.size());
      for (const auto& aj : m_jobs) {
        rows.emplace_back(makeJobRow(aj));
      }
      postgres::ArchiveJobQueueRow::insertBunch(m_conn, rows, false);
      rows.back().addParamsToLogContext(params);
      m_lc.log(log::INFO, "In ArchiveRequest::insert(): added jobs to queue.");
    }
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In ArchiveRequest::insert(): failed to queue job.");
    m_conn.rollback();  // Rollback on error
    throw;
  }
}

//void ArchiveRequest::insert() {
//  // Getting the next ID for the request possibly composed of multiple jobs
//  uint64_t areq_id = cta::schedulerdb::postgres::ArchiveJobQueueRow::getNextArchiveRequestID(m_conn);
//  uint32_t areq_job_count = m_jobs.size();
//  for (const auto& aj : m_jobs) {
//    try {
//      cta::schedulerdb::postgres::ArchiveJobQueueRow ajr;
//      if (areq_job_count == 1) {
//        ajr.diskFileId = std::move(m_archiveFile.diskFileId);
//        ajr.diskInstance = std::move(m_archiveFile.diskInstance);
//        ajr.storageClass = std::move(m_archiveFile.storageClass);
//        ajr.diskFileInfoPath = std::move(m_archiveFile.diskFileInfo.path);
//        ajr.checksumBlob = std::move(m_archiveFile.checksumBlob);
//        ajr.archiveReportURL = std::move(m_archiveReportURL);
//        ajr.archiveErrorReportURL = std::move(m_archiveErrorReportURL);
//        ajr.srcUrl = std::move(m_srcURL);
//      } else {
//        ajr.diskFileId = m_archiveFile.diskFileId;
//        ajr.diskInstance = m_archiveFile.diskInstance;
//        ajr.storageClass = m_archiveFile.storageClass;
//        ajr.diskFileInfoPath = m_archiveFile.diskFileInfo.path;
//        ajr.checksumBlob = m_archiveFile.checksumBlob;
//        ajr.archiveReportURL = m_archiveReportURL;
//        ajr.archiveErrorReportURL = m_archiveErrorReportURL;
//        ajr.srcUrl = m_srcURL;
//      }
//      ajr.reqId = areq_id;
//      ajr.reqJobCount = areq_job_count;
//      ajr.tapePool = aj.tapepool;
//      ajr.mountPolicy = m_mountPolicy.name;
//      ajr.priority = m_mountPolicy.archivePriority;
//      ajr.minArchiveRequestAge = m_mountPolicy.archiveMinRequestAge;
//      ajr.archiveFileID = m_archiveFile.archiveFileID;
//      ajr.fileSize = m_archiveFile.fileSize;
//      ajr.diskFileInfoOwnerUid = m_archiveFile.diskFileInfo.owner_uid;
//      ajr.diskFileInfoGid = m_archiveFile.diskFileInfo.gid;
//
//      ajr.creationTime = m_entryLog.time;  // Time the job was received by the CTA Frontend
//      ajr.copyNb = aj.copyNb;
//      ajr.startTime = time(nullptr);  // Time the job was queued in the DB
//      ajr.requesterName = m_requesterIdentity.name;
//      ajr.requesterGroup = m_requesterIdentity.group;
//      ajr.retriesWithinMount = aj.retriesWithinMount;
//      ajr.maxRetriesWithinMount = aj.maxRetriesWithinMount;
//      ajr.maxReportRetries = aj.maxReportRetries;
//      ajr.totalRetries = aj.totalRetries;
//      ajr.lastMountWithFailure = aj.lastMountWithFailure;
//      ajr.maxTotalRetries = aj.maxTotalRetries;
//
//      log::ScopedParamContainer params(m_lc);
//      ajr.addParamsToLogContext(params);
//      // Inserting the jobs to the DB
//      ajr.insert(m_conn);
//      // m_conn.commit(); unnecessary for insert !
//    } catch (exception::Exception& ex) {
//      log::ScopedParamContainer params(m_lc);
//      params.add("exceptionMessage", ex.getMessageValue());
//      m_lc.log(log::ERR, "In ArchiveRequest::insert(): failed to queue request.");
//      m_conn.rollback();  // Rollback on error
//      throw;
//    }
//  }
//  m_lc.log(log::INFO, "In ArchiveRequest::insert(): added jobs to queue.");
//}

void ArchiveRequest::addJob(uint8_t copyNumber,
                            std::string_view tapepool,
                            uint16_t maxRetriesWithinMount,
                            uint16_t maxTotalRetries,
                            uint16_t maxReportRetries) {
  Job newJob;
  newJob.copyNb = copyNumber;
  newJob.status = ArchiveJobStatus::AJS_ToTransferForUser;
  newJob.tapepool = tapepool;
  newJob.totalRetries = 0;
  newJob.retriesWithinMount = 0;
  newJob.lastMountWithFailure = 0;
  newJob.maxRetriesWithinMount = maxRetriesWithinMount;
  newJob.maxTotalRetries = maxTotalRetries;
  newJob.totalReportRetries = 0;
  newJob.maxReportRetries = maxReportRetries;
  m_jobs.push_back(newJob);
}

void ArchiveRequest::setArchiveFile(common::dataStructures::ArchiveFile archiveFile) {
  m_archiveFile = std::move(archiveFile);
}

common::dataStructures::ArchiveFile ArchiveRequest::getArchiveFile() const {
  return m_archiveFile;
}

void ArchiveRequest::setArchiveReportURL(std::string_view URL) {
  m_archiveReportURL = URL;
}

std::string ArchiveRequest::getArchiveReportURL() const {
  return m_archiveReportURL;
}

void ArchiveRequest::setArchiveErrorReportURL(std::string_view URL) {
  m_archiveErrorReportURL = URL;
}

std::string ArchiveRequest::getArchiveErrorReportURL() const {
  return m_archiveErrorReportURL;
}

void ArchiveRequest::setRequester(const common::dataStructures::RequesterIdentity& requester) {
  m_requesterIdentity = requester;
}

common::dataStructures::RequesterIdentity ArchiveRequest::getRequester() const {
  return m_requesterIdentity;
}

void ArchiveRequest::setSrcURL(std::string_view srcURL) {
  m_srcURL = srcURL;
}

std::string ArchiveRequest::getSrcURL() const {
  return m_srcURL;
}

void ArchiveRequest::setEntryLog(const common::dataStructures::EntryLog& creationLog) {
  m_entryLog = creationLog;
}

common::dataStructures::EntryLog ArchiveRequest::getEntryLog() const {
  return m_entryLog;
}

void ArchiveRequest::setMountPolicy(const common::dataStructures::MountPolicy& mountPolicy) {
  m_mountPolicy = mountPolicy;
}

common::dataStructures::MountPolicy ArchiveRequest::getMountPolicy() const {
  return m_mountPolicy;
}

[[noreturn]] std::list<ArchiveRequest::JobDump> ArchiveRequest::dumpJobs() const {
  throw std::runtime_error("dumpJobs not implemented.");
}

}  // namespace cta::schedulerdb
