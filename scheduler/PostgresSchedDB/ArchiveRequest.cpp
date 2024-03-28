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

#include "scheduler/PostgresSchedDB/ArchiveRequest.hpp"
#include "scheduler/PostgresSchedDB/sql/ArchiveJobQueue.hpp"
#include "common/Timer.hpp"

namespace cta::postgresscheddb {

[[noreturn]] void ArchiveRequest::update() const {
  throw std::runtime_error("update not implemented.");
}

void ArchiveRequest::insert() {
  m_txn.reset(new postgresscheddb::Transaction(m_connPool));

  for(const auto &aj : m_jobs) {
    cta::postgresscheddb::sql::ArchiveJobQueueRow ajr;

    ajr.tapePool = aj.tapepool;
    ajr.mountPolicy = m_mountPolicy.name;
    ajr.priority = m_mountPolicy.archivePriority;
    ajr.minArchiveRequestAge = m_mountPolicy.archiveMinRequestAge;
    ajr.archiveFile = m_archiveFile;
    ajr.archiveFile.creationTime = m_entryLog.time; // Time the job was received by the CTA Frontend
    ajr.copyNb = aj.copyNb;
    ajr.startTime = time(nullptr); // Time the job was queued in the DB
    ajr.archiveReportUrl = m_archiveReportURL;
    ajr.archiveErrorReportUrl = m_archiveErrorReportURL;
    ajr.requesterName = m_requesterIdentity.name;
    ajr.requesterGroup = m_requesterIdentity.group;
    ajr.srcUrl = m_srcURL;
    ajr.retriesWithinMount = aj.retriesWithinMount;
    ajr.totalRetries = aj.totalRetries;
    ajr.lastMountWithFailure = aj.lastMountWithFailure;
    ajr.maxTotalRetries = aj.maxTotalRetries;

    log::ScopedParamContainer params(m_lc);
    //ajr.addParamsToLogContext(params);

    try {
      cta::utils::Timer timerinsert;
      for (int i = 0; i < 100000; ++i) {
        ajr.insert(*m_txn);
      }
      params.add("Insert10to5TimeSec", timerinsert.secs());
      m_lc.log(log::DEBUG, "In PostgresSchedDB::ArchiveRequest::insert(): 100 000x insert().");
      cta::utils::Timer timerupdate;
      for (int i = 0; i < 10; ++i) {
        ajr.update(*m_txn);
      }
      params.add("Update10to5TimeSec", timerupdate.secs());
      m_lc.log(log::DEBUG, "In PostgresSchedDB::ArchiveRequest::update(): 10x update().");
      cta::utils::Timer timerinsert2;
      for (int i = 0; i < 1000000; ++i) {
        ajr.insert(*m_txn);
      }
      params.add("Insert10to6TimeSec", timerinsert2.secs());
      m_lc.log(log::DEBUG, "In PostgresSchedDB::ArchiveRequest::insert(): 1 000 000x insert().");

      cta::utils::Timer timerdelete;
      for (int i = 0; i < 100000; ++i) {
        ajr.delete(*m_txn);
      }
      params.add("Delete10to5TimeSec", timerdelete.secs());
      m_lc.log(log::DEBUG, "In PostgresSchedDB::ArchiveRequest::insert(): 100 000x delete().");

      cta::utils::Timer timerdelete2;
      for (int i = 0; i < 1000000; ++i) {
        ajr.delete(*m_txn);
      }
      params.add("Delete10to6TimeSec", timerdelete2.secs());
      m_lc.log(log::DEBUG, "In PostgresSchedDB::ArchiveRequest::insert(): 1 000 000x delete().");

      // ajr.update(*m_txn); //for testing only
    } catch(exception::Exception &ex) {
      params.add("exeptionMessage", ex.getMessageValue());
      m_lc.log(log::ERR, "In ArchiveRequest::insert(): failed to queue job.");
      throw;
    }

    //m_lc.log(log::INFO, "In ArchiveRequest::insert(): added job to queue.");
  }
}

void ArchiveRequest::commit() {
  if (m_txn) {
    m_txn->commit();
  }
  m_txn.reset();
}

void ArchiveRequest::addJob(uint8_t copyNumber, std::string_view tapepool, uint16_t maxRetriesWithinMount, uint16_t maxTotalRetries, uint16_t maxReportRetries) {
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

void ArchiveRequest::setArchiveFile(const common::dataStructures::ArchiveFile& archiveFile) {
  m_archiveFile = archiveFile;
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

} // namespace cta::postgresscheddb
