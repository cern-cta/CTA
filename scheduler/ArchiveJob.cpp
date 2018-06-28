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
#include <cryptopp/base64.h>

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
// asyncReportComplete
//------------------------------------------------------------------------------
void cta::ArchiveJob::asyncReportComplete() {
  m_reporter.reset(m_mount.createDiskReporter(m_dbJob->archiveReportURL, m_reporterState));
  m_reporter->asyncReportArchiveFullyComplete();
  m_reporterTimer.reset();
}

//------------------------------------------------------------------------------
// getReportTiming()
//------------------------------------------------------------------------------
double cta::ArchiveJob::reportTime() {
  return m_reporterTimer.secs();
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
void cta::ArchiveJob::failed(const std::string &failureReason,  log::LogContext & lc) {
  if (m_dbJob->fail(failureReason, lc)) {
    std::string base64ErrorReport;
    // Construct a pipe: msg -> sign -> Base64 encode -> result goes into ret.
    const bool noNewLineInBase64Output = false;
    CryptoPP::StringSource ss1(failureReason, true, 
        new CryptoPP::Base64Encoder(
          new CryptoPP::StringSink(base64ErrorReport), noNewLineInBase64Output));
    std::string fullReportURL = m_dbJob->errorReportURL + base64ErrorReport;
    // That's all job's already done.
    std::promise<void> reporterState;
    utils::Timer t;
    std::unique_ptr<cta::eos::DiskReporter> reporter(m_mount.createDiskReporter(fullReportURL, reporterState));
    reporter->asyncReportArchiveFullyComplete();
    try {
      reporterState.get_future().get();
      log::ScopedParamContainer params(lc);
      params.add("fileId", m_dbJob->archiveFile.archiveFileID)
            .add("diskInstance", m_dbJob->archiveFile.diskInstance)
            .add("diskFileId", m_dbJob->archiveFile.diskFileId)
            .add("fullReportURL", fullReportURL)
            .add("errorReport", failureReason)
            .add("reportTime", t.secs());
      lc.log(log::INFO, "In ArchiveJob::failed(): reported error to client.");
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params(lc);
      params.add("fileId", m_dbJob->archiveFile.archiveFileID)
            .add("diskInstance", m_dbJob->archiveFile.diskInstance)
            .add("diskFileId", m_dbJob->archiveFile.diskFileId)
            .add("errorReport", failureReason)
            .add("exceptionMsg", ex.getMessageValue())
            .add("reportTime", t.secs());
      lc.log(log::ERR, "In ArchiveJob::failed(): failed to report error to client.");
      lc.logBacktrace(log::ERR, ex.backtrace());
    }
  }
}

//------------------------------------------------------------------------------
// waitForReporting
//------------------------------------------------------------------------------
void cta::ArchiveJob::waitForReporting() {
  m_reporterState.get_future().get();
}
