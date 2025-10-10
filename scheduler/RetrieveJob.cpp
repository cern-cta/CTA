/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/RetrieveJob.hpp"
#include "common/Timer.hpp"
#include "disk/DiskReporter.hpp"
#include "RetrieveMount.hpp"

//------------------------------------------------------------------------------
// diskSystemName
//------------------------------------------------------------------------------
std::optional<std::string> cta::RetrieveJob::diskSystemName() {
  return m_dbJob->diskSystemName;
}

//------------------------------------------------------------------------------
// asyncComplete
//------------------------------------------------------------------------------
void cta::RetrieveJob::asyncSetSuccessful() {
  m_dbJob->asyncSetSuccessful();
}

//------------------------------------------------------------------------------
// exceptionThrowingReportURL
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::exceptionThrowingReportURL() {
  switch (m_dbJob->reportType) {
    case SchedulerDatabase::RetrieveJob::ReportType::CompletionReport:
      return retrieveRequest.retrieveReportURL;
    case SchedulerDatabase::RetrieveJob::ReportType::FailureReport:
      return retrieveRequest.errorReportURL;
    case SchedulerDatabase::RetrieveJob::ReportType::NoReportRequired:
      throw exception::Exception("In RetrieveJob::exceptionThrowingReportURL(): job status NoReportRequired does not require reporting.");
    case SchedulerDatabase::RetrieveJob::ReportType::Report:
      throw exception::Exception("In RetrieveJob::exceptionThrowingReportURL(): job status Report does not require reporting.");
  }
  throw exception::Exception("In RetrieveJob::exceptionThrowingReportURL(): invalid report type reportType=" +
                             std::to_string(static_cast<uint8_t>(m_dbJob->reportType)));
}

//------------------------------------------------------------------------------
// reportType
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::reportType() {
  switch(m_dbJob->reportType) {
    case SchedulerDatabase::RetrieveJob::ReportType::CompletionReport:
      return "CompletionReport";
    case SchedulerDatabase::RetrieveJob::ReportType::FailureReport:
      return "FailureReport";
    default:
      throw exception::Exception("In RetrieveJob::reportType(): job status does not require reporting.");
  }
}

//------------------------------------------------------------------------------
// RetrieveJob::getJobID
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::getJobID() {
  return std::to_string(m_dbJob->jobID);
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

//------------------------------------------------------------------------------
// selectedTapeFile
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeFile& cta::RetrieveJob::selectedTapeFile() {
  return archiveFile.tapeFiles.at(selectedCopyNb);
}

//------------------------------------------------------------------------------
// selectedTapeFile
//------------------------------------------------------------------------------
const cta::common::dataStructures::TapeFile& cta::RetrieveJob::selectedTapeFile() const {
  return archiveFile.tapeFiles.at(selectedCopyNb);
}
