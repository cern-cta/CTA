/*
 * The CERN Tape Retrieve (CTA) project
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

#include "scheduler/RetrieveJob.hpp"
#include "common/Timer.hpp"
#include "eos/DiskReporter.hpp"
#include "RetrieveMount.hpp"
#include <cryptopp/base64.h>
#include <future>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveJob::~RetrieveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveJob::RetrieveJob(RetrieveMount *mount,
  const common::dataStructures::RetrieveRequest &retrieveRequest,
  const common::dataStructures::ArchiveFile & archiveFile,
  const uint64_t selectedCopyNb,
  const PositioningMethod positioningMethod):
  m_mount(mount),
  retrieveRequest(retrieveRequest),
  archiveFile(archiveFile),
  selectedCopyNb(selectedCopyNb),
  positioningMethod(positioningMethod),
  transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) {}

//------------------------------------------------------------------------------
// asyncComplete
//------------------------------------------------------------------------------
void cta::RetrieveJob::asyncComplete() {
  m_dbJob->asyncSucceed();
}

//------------------------------------------------------------------------------
// checkComplete
//------------------------------------------------------------------------------
void cta::RetrieveJob::checkComplete() {
  m_dbJob->checkSucceed();
}

//------------------------------------------------------------------------------
// ArchiveJob::reportURL
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::reportURL() {
  throw std::runtime_error("RetrieveJob::reportURL(): not implemented");
#if 0
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
#endif
}

//------------------------------------------------------------------------------
// ArchiveJob::reportType
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::reportType() {
  throw std::runtime_error("RetrieveJob::reportType(): not implemented");
#if 0
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
#endif
}

//------------------------------------------------------------------------------
// reportFailed
//------------------------------------------------------------------------------
void cta::RetrieveJob::reportFailed(const std::string &failureReason, log::LogContext &lc) {
  // This is fully delegated to the DB, which will handle the queueing for next steps (if any)
  m_dbJob->failReport(failureReason, lc);
}


//------------------------------------------------------------------------------
// transferFailed
//------------------------------------------------------------------------------
void cta::RetrieveJob::transferFailed(const std::string &failureReason, log::LogContext &lc) {
  // This is fully delegated to the DB, which will handle the queueing for next steps (if any)
  m_dbJob->failTransfer(failureReason, lc);
}

#if 0
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::RetrieveJob::failed(const std::string & failureReason, log::LogContext &lc) {
  if (m_dbJob->fail(failureReason, lc)) {
    std::string base64ErrorReport;
    // Construct a pipe: msg -> sign -> Base64 encode -> result goes into ret.
    const bool noNewLineInBase64Output = false;
    CryptoPP::StringSource ss1(failureReason, true, 
        new CryptoPP::Base64Encoder(
          new CryptoPP::StringSink(base64ErrorReport), noNewLineInBase64Output));
    std::string fullReportURL = m_dbJob->retrieveRequest.errorReportURL + base64ErrorReport;
    // That's all job's already done.
    std::promise<void> reporterState;
    utils::Timer t;
    std::unique_ptr<cta::eos::DiskReporter> reporter(m_mount.createDiskReporter(fullReportURL));
    reporter->asyncReport();
    try {
      reporter->waitReport();
      log::ScopedParamContainer params(lc);
      params.add("fileId", m_dbJob->archiveFile.archiveFileID)
            .add("diskInstance", m_dbJob->archiveFile.diskInstance)
            .add("diskFileId", m_dbJob->archiveFile.diskFileId)
            .add("errorReport", failureReason)
            .add("reportTime", t.secs());
      lc.log(log::INFO, "In RetrieveJob::failed(): reported error to client.");
    } catch (cta::exception::Exception & ex) {
      log::ScopedParamContainer params(lc);
      params.add("fileId", m_dbJob->archiveFile.archiveFileID)
            .add("diskInstance", m_dbJob->archiveFile.diskInstance)
            .add("diskFileId", m_dbJob->archiveFile.diskFileId)
            .add("errorReport", failureReason)
            .add("exceptionMsg", ex.getMessageValue())
            .add("reportTime", t.secs());
      lc.log(log::ERR, "In RetrieveJob::failed(): failed to report error to client.");
      lc.logBacktrace(log::ERR, ex.backtrace());
    }
  }
}
#endif

//------------------------------------------------------------------------------
// selectedTapeFile
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeFile& cta::RetrieveJob::selectedTapeFile() {
  try {
    return archiveFile.tapeFiles.at(selectedCopyNb);
  } catch (std::out_of_range &ex) {
    auto __attribute__((__unused__)) & debug=ex;
    throw std::runtime_error(std::string("cta::RetrieveJob::selectedTapeFile(): ") + ex.what());
  }
}

//------------------------------------------------------------------------------
// selectedTapeFile
//------------------------------------------------------------------------------
const cta::common::dataStructures::TapeFile& cta::RetrieveJob::selectedTapeFile() const {
  try {
    return archiveFile.tapeFiles.at(selectedCopyNb);
  } catch (std::out_of_range &ex) {
    auto __attribute__((__unused__)) & debug=ex;
    throw std::runtime_error(std::string("cta::RetrieveJob::selectedTapeFile(): ") + ex.what());
  }
}

