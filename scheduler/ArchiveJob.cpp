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
#include "common/make_unique.hpp"
#include <limits>
#include <cryptopp/base64.h>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveJob::~ArchiveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(ArchiveMount *mount,
  catalogue::Catalogue & catalogue,
  const common::dataStructures::ArchiveFile &archiveFile,
  const std::string &srcURL,
  const common::dataStructures::TapeFile &tapeFile):
  m_mount(mount), m_catalogue(catalogue),
  archiveFile(archiveFile),
  srcURL(srcURL),
  tapeFile(tapeFile) {}

//------------------------------------------------------------------------------
// getReportTiming()
//------------------------------------------------------------------------------
double cta::ArchiveJob::reportTime() {
  return m_reporterTimer.secs();
}

//------------------------------------------------------------------------------
// ArchiveJob::validateAndGetTapeFileWritten
//------------------------------------------------------------------------------
cta::catalogue::TapeItemWrittenPointer cta::ArchiveJob::validateAndGetTapeFileWritten() {
  validate();
  auto fileReportUP = cta::make_unique<catalogue::TapeFileWritten>();
  auto & fileReport = *fileReportUP;
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
  fileReport.diskInstance = archiveFile.diskInstance;
  fileReport.fSeq = tapeFile.fSeq;
  fileReport.size = archiveFile.fileSize;
  fileReport.storageClassName = archiveFile.storageClass;
  fileReport.tapeDrive = getMount().getDrive();
  fileReport.vid = tapeFile.vid;
  return cta::catalogue::TapeItemWrittenPointer(fileReportUP.release());
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
  switch (m_dbJob->reportType) {
  case SchedulerDatabase::ArchiveJob::ReportType::CompletionReport:
    return m_dbJob->archiveReportURL;
  case SchedulerDatabase::ArchiveJob::ReportType::FailureReport:
    {
      if (m_dbJob->latestError.empty()) {
        throw exception::Exception("In ArchiveJob::reportURL(): empty failure reason.");
      }
      std::string base64ErrorReport;
      // Construct a pipe: msg -> sign -> Base64 encode -> result goes into ret.
      const bool noNewLineInBase64Output = false;
      CryptoPP::StringSource ss1(m_dbJob->latestError, true, 
        new CryptoPP::Base64Encoder(
          new CryptoPP::StringSink(base64ErrorReport), noNewLineInBase64Output));
      return m_dbJob->errorReportURL + base64ErrorReport;
    }
  default:
    { 
      throw exception::Exception("In ArchiveJob::reportURL(): job status does not require reporting.");
    }
  }
}

//------------------------------------------------------------------------------
// ArchiveJob::reportType
//------------------------------------------------------------------------------
std::string cta::ArchiveJob::reportType() {
  switch (m_dbJob->reportType) {
  case SchedulerDatabase::ArchiveJob::ReportType::CompletionReport:
    return "CompletionReport";
  case SchedulerDatabase::ArchiveJob::ReportType::FailureReport:
    return "ErrorReport";
  default:
    { 
      throw exception::Exception("In ArchiveJob::reportType(): job status does not require reporting.");
    }
  }
}

//------------------------------------------------------------------------------
// ArchiveJob::reportFailed
//------------------------------------------------------------------------------
void cta::ArchiveJob::reportFailed(const std::string& failureReason, log::LogContext& lc) {
  // This is fully delegated to the DB, which will handle the queueing for next steps, if any.
  m_dbJob->failReport(failureReason, lc);
}


//------------------------------------------------------------------------------
// ArchiveJob::transferFailed
//------------------------------------------------------------------------------
void cta::ArchiveJob::transferFailed(const std::string &failureReason,  log::LogContext & lc) {
  // This is fully delegated to the DB, which will handle the queueing for next steps, if any.
  m_dbJob->failTransfer(failureReason, lc);
}

//------------------------------------------------------------------------------
// waitForReporting
//------------------------------------------------------------------------------
void cta::ArchiveJob::waitForReporting() {
  m_reporterState.get_future().get();
}

//------------------------------------------------------------------------------
// cta::ArchiveJob::getMount()
//------------------------------------------------------------------------------
cta::ArchiveMount& cta::ArchiveJob::getMount() {
  if (m_mount) return *m_mount;
  throw exception::Exception("In ArchiveJob::getMount(): no mount set.");
}
