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

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveJob::~RetrieveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveJob::RetrieveJob(RetrieveMount &mount,
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
// complete
//------------------------------------------------------------------------------
void cta::RetrieveJob::complete() {
  m_dbJob->succeed();
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::RetrieveJob::failed() {
  throw std::runtime_error("cta::RetrieveJob::failed(): not implemented");
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::RetrieveJob::retry() {
  throw std::runtime_error("cta::RetrieveJob::retry(): not implemented");
}

//------------------------------------------------------------------------------
// selectedTapeFile
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeFile& cta::RetrieveJob::selectedTapeFile() {
  try {
    return archiveFile.tapeFiles.at(selectedCopyNb);
  } catch (std::out_of_range &ex) {
    auto __attribute__((__unused__)) & debug=ex;
    throw;
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
    throw;
  }
}

