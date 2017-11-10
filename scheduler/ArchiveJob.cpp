/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "scheduler/ArchiveJob.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "eos/DiskReporterFactory.hpp"
#include <limits>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveJob::~ArchiveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(ArchiveMount &mount,
  catalogue::Catalogue & catalogue,
  const common::dataStructures::ArchiveFile &archiveFile,
  const std::string &srcURL,
  const common::dataStructures::TapeFile &tapeFile):
  m_mount(mount), m_catalogue(catalogue),
  archiveFile(archiveFile),
  srcURL(srcURL),
  tapeFile(tapeFile) {}

//------------------------------------------------------------------------------
// asyncSetJobSucceed
//------------------------------------------------------------------------------
void cta::ArchiveJob::asyncSetJobSucceed() {
  m_dbJob->asyncSucceed();
}

//------------------------------------------------------------------------------
// asyncSetJobSucceed
//------------------------------------------------------------------------------
void cta::ArchiveJob::asyncSucceedAndWaitJobsBatch(std::list<std::unique_ptr<cta::ArchiveJob> >& jobs) {
  // Call succeed on all jobs
  for (auto & j: jobs) {
    j->asyncSetJobSucceed();
  }
}


//------------------------------------------------------------------------------
// asyncSetJobsSucceed
//------------------------------------------------------------------------------
void cta::ArchiveJob::asyncSetJobsBatchSucceed(std::list<std::unique_ptr<cta::ArchiveJob>> & jobs) {
  // We need a handle on the mount (all jobs are supposed to come from the same mount.
  // It will be provided indirectly by a non-static member function of one job (if any).
  if (jobs.size()) {
    jobs.front()->asyncSucceedAndWaitJobsBatch(jobs);
  }
}

//------------------------------------------------------------------------------
// checkAndReportComplete
//------------------------------------------------------------------------------
bool cta::ArchiveJob::checkAndAsyncReportComplete() {
  if (m_dbJob->checkSucceed()) {
    m_reporter.reset(m_mount.createDiskReporter(m_dbJob->archiveReportURL, m_reporterState));
    m_reporter->asyncReportArchiveFullyComplete();
    m_reporterTimer.reset();
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// getReportTiming()
//------------------------------------------------------------------------------
double cta::ArchiveJob::reportTime() {
  return m_reporterTimer.secs();
}

//------------------------------------------------------------------------------
// ArchiveJob::writeToCatalogue
//------------------------------------------------------------------------------
void cta::ArchiveJob::writeToCatalogue() {
  catalogue::TapeFileWritten fileReport;
  fileReport.archiveFileId = archiveFile.archiveFileID;
  fileReport.blockId = tapeFile.blockId;
  fileReport.checksumType = tapeFile.checksumType;
  fileReport.checksumValue = tapeFile.checksumValue;
  fileReport.compressedSize = tapeFile.compressedSize;
  fileReport.copyNb = tapeFile.copyNb;
  fileReport.diskFileId = archiveFile.diskFileId;
  fileReport.diskFileUser = archiveFile.diskFileInfo.owner;
  fileReport.diskFileGroup = archiveFile.diskFileInfo.group;
  fileReport.diskFilePath = archiveFile.diskFileInfo.path;
  fileReport.diskFileRecoveryBlob = archiveFile.diskFileInfo.recoveryBlob;
  fileReport.diskInstance = archiveFile.diskInstance;
  fileReport.fSeq = tapeFile.fSeq;
  fileReport.size = archiveFile.fileSize;
  fileReport.storageClassName = archiveFile.storageClass;
  fileReport.tapeDrive = m_mount.getDrive();
  fileReport.vid = tapeFile.vid;
  m_catalogue.filesWrittenToTape (std::set<catalogue::TapeFileWritten>{fileReport});
}
//------------------------------------------------------------------------------
// ArchiveJob::validateAndGetTapeFileWritten
//------------------------------------------------------------------------------
cta::catalogue::TapeFileWritten cta::ArchiveJob::validateAndGetTapeFileWritten() {
  validate();
  catalogue::TapeFileWritten fileReport;
  fileReport.archiveFileId = archiveFile.archiveFileID;
  fileReport.blockId = tapeFile.blockId;
  fileReport.checksumType = tapeFile.checksumType;
  fileReport.checksumValue = tapeFile.checksumValue;
  fileReport.compressedSize = tapeFile.compressedSize;
  fileReport.copyNb = tapeFile.copyNb;
  fileReport.diskFileId = archiveFile.diskFileId;
  fileReport.diskFileUser = archiveFile.diskFileInfo.owner;
  fileReport.diskFileGroup = archiveFile.diskFileInfo.group;
  fileReport.diskFilePath = archiveFile.diskFileInfo.path;
  fileReport.diskFileRecoveryBlob = archiveFile.diskFileInfo.recoveryBlob;
  fileReport.diskInstance = archiveFile.diskInstance;
  fileReport.fSeq = tapeFile.fSeq;
  fileReport.size = archiveFile.fileSize;
  fileReport.storageClassName = archiveFile.storageClass;
  fileReport.tapeDrive = m_mount.getDrive();
  fileReport.vid = tapeFile.vid;
  return fileReport;
}

//------------------------------------------------------------------------------
// ArchiveJob::validate
//------------------------------------------------------------------------------
void cta::ArchiveJob::validate(){
  // First check that the block Id for the file has been set.
  if (tapeFile.blockId ==
      std::numeric_limits<decltype(tapeFile.blockId)>::max())
    throw BlockIdNotSet("In cta::ArchiveJob::validate(): Block ID not set");
  // Also check the checksum has been set
  if (archiveFile.checksumType.empty() || archiveFile.checksumValue.empty() || 
      tapeFile.checksumType.empty() || tapeFile.checksumValue.empty())
    throw ChecksumNotSet("In cta::ArchiveJob::validate(): checksums not set");
  // And matches
  if (archiveFile.checksumType != tapeFile.checksumType || 
      archiveFile.checksumValue != tapeFile.checksumValue)
    throw ChecksumMismatch(std::string("In cta::ArchiveJob::validate(): checksum mismatch!")
            +" Archive file checksum type: "+archiveFile.checksumType
            +" Archive file checksum value: "+archiveFile.checksumValue
            +" Tape file checksum type: "+tapeFile.checksumType
            +" Tape file checksum value: "+tapeFile.checksumValue);
}

//------------------------------------------------------------------------------
// ArchiveJob::reportURL
//------------------------------------------------------------------------------
std::string cta::ArchiveJob::reportURL() {
  return m_dbJob->archiveReportURL;
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::ArchiveJob::failed(const cta::exception::Exception &ex,  log::LogContext & lc) {
  m_dbJob->fail(lc);
}

//------------------------------------------------------------------------------
// waitForReporting
//------------------------------------------------------------------------------
void cta::ArchiveJob::waitForReporting() {
  m_reporterState.get_future().get();
}
