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

#include "scheduler/RetrieveJob.hpp"
#include "common/Timer.hpp"
#include "disk/DiskReporter.hpp"
#include "RetrieveMount.hpp"
#include <cryptopp/base64.h>
#include <future>

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveJob::~RetrieveJob() throw() {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveJob::RetrieveJob(RetrieveMount* mount,
                              const common::dataStructures::RetrieveRequest& retrieveRequest,
                              const common::dataStructures::ArchiveFile& archiveFile,
                              const uint64_t selectedCopyNb,
                              const PositioningMethod positioningMethod) :
m_mount(mount),
retrieveRequest(retrieveRequest),
archiveFile(archiveFile),
selectedCopyNb(selectedCopyNb),
positioningMethod(positioningMethod),
transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) {}

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
// reportType
//------------------------------------------------------------------------------
std::string cta::RetrieveJob::reportType() {
  switch (m_dbJob->reportType) {
    case SchedulerDatabase::RetrieveJob::ReportType::FailureReport:
      return "FailureReport";
    default:
      throw exception::Exception("In RetrieveJob::reportType(): job status does not require reporting.");
  }
}

//------------------------------------------------------------------------------
// reportFailed
//------------------------------------------------------------------------------
void cta::RetrieveJob::reportFailed(const std::string& failureReason, log::LogContext& lc) {
  // This is fully delegated to the DB, which will handle the queueing for next steps (if any)
  m_dbJob->failReport(failureReason, lc);
}

//------------------------------------------------------------------------------
// transferFailed
//------------------------------------------------------------------------------
void cta::RetrieveJob::transferFailed(const std::string& failureReason, log::LogContext& lc) {
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
