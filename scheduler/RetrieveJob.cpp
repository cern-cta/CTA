/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "scheduler/RetrieveJob.hpp"
#include "common/Timer.hpp"
#include "disk/DiskReporter.hpp"
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
// diskSystemName
//------------------------------------------------------------------------------
cta::optional<std::string> cta::RetrieveJob::diskSystemName() {
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
  switch(m_dbJob->reportType) {
    case SchedulerDatabase::RetrieveJob::ReportType::FailureReport:
      return "FailureReport";
    default:
      throw exception::Exception("In RetrieveJob::reportType(): job status does not require reporting.");
  }
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
