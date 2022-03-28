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

#include "scheduler/ArchiveJob.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "disk/DiskReporterFactory.hpp"
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
                            catalogue::Catalogue& catalogue,
                            const common::dataStructures::ArchiveFile& archiveFile,
                            const std::string& srcURL,
                            const common::dataStructures::TapeFile& tapeFile) :
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
  auto& fileReport = *fileReportUP;
  fileReport.archiveFileId = archiveFile.archiveFileID;
  fileReport.blockId = tapeFile.blockId;
  fileReport.checksumBlob = tapeFile.checksumBlob;
  fileReport.copyNb = tapeFile.copyNb;
  fileReport.diskFileId = archiveFile.diskFileId;
  fileReport.diskFileOwnerUid = archiveFile.diskFileInfo.owner_uid;
  fileReport.diskFileGid = archiveFile.diskFileInfo.gid;
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
void cta::ArchiveJob::validate() {
  // First check that the block Id for the file has been set.
  if (tapeFile.blockId == std::numeric_limits<decltype(tapeFile.blockId)>::max())
    throw BlockIdNotSet("In cta::ArchiveJob::validate(): Block ID not set");
  // Also check the checksum has been set
  if (archiveFile.checksumBlob.empty() || tapeFile.checksumBlob.empty())
    throw ChecksumNotSet("In cta::ArchiveJob::validate(): checksums not set");
  // And matches
  archiveFile.checksumBlob.validate(tapeFile.checksumBlob);
}

//------------------------------------------------------------------------------
// ArchiveJob::exceptionThrowingReportURL
//------------------------------------------------------------------------------
std::string cta::ArchiveJob::exceptionThrowingReportURL() {
  switch (m_dbJob->reportType) {
    case SchedulerDatabase::ArchiveJob::ReportType::CompletionReport:
      return m_dbJob->archiveReportURL;
    case SchedulerDatabase::ArchiveJob::ReportType::FailureReport: {
      if (m_dbJob->latestError.empty()) {
        throw exception::Exception("In ArchiveJob::exceptionThrowingReportURL(): empty failure reason.");
      }
      std::string base64ErrorReport;
      // Construct a pipe: msg -> sign -> Base64 encode -> result goes into ret.
      const bool noNewLineInBase64Output = false;
      CryptoPP::StringSource ss1(m_dbJob->latestError, true,
                                 new CryptoPP::Base64Encoder(
                                   new CryptoPP::StringSink(base64ErrorReport), noNewLineInBase64Output));
      return m_dbJob->errorReportURL + base64ErrorReport;
    }
    case SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired:
      throw exception::Exception("In ArchiveJob::exceptionThrowingReportURL(): job status NoReportRequired does not require reporting.");
    case SchedulerDatabase::ArchiveJob::ReportType::Report:
      throw exception::Exception("In ArchiveJob::exceptionThrowingReportURL(): job status Report does not require reporting.");
  }
  throw exception::Exception("In ArchiveJob::exceptionThrowingReportURL(): invalid report type reportType=" +
                             std::to_string(static_cast<uint8_t>(m_dbJob->reportType)));
}

//------------------------------------------------------------------------------
// ArchiveJob::reportURL
//------------------------------------------------------------------------------
std::string cta::ArchiveJob::reportURL() noexcept {
  try {
    return exceptionThrowingReportURL();
  } catch (exception::Exception& ex) {
    return ex.what();
  } catch (...) {
    return "In ArchiveJob::reportURL(): unknown exception";
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
      return "FailureReport";
    case SchedulerDatabase::ArchiveJob::ReportType::NoReportRequired:
      return "NoReportRequired";
    case SchedulerDatabase::ArchiveJob::ReportType::Report:
      return "Report";
    default: {
      throw exception::Exception("In ArchiveJob::reportType(): job status does not require reporting.");
    }
  }
  throw exception::Exception("In ArchiveJob::reportType(): invalid report type.");
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
void cta::ArchiveJob::transferFailed(const std::string& failureReason, log::LogContext& lc) {
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
