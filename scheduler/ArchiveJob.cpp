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

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveJob::~ArchiveJob() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveJob::ArchiveJob(
// NOT YET DECIDED
//ArchiveMount &mount,
  const std::string &tapePoolName,
  const uint64_t fileSize,
  const std::string &id,
  const std::string &userRequestId,
  const uint32_t copyNb,
  const std::string &remoteFile,
  const uint64_t castorNsFileId):
  TapeJob(id, userRequestId, copyNb, remoteFile, castorNsFileId),
// NOT YET DECIDED
//  m_mount(mount),
  m_tapePoolName(tapePoolName),
  m_fileSize(fileSize) {
}

//------------------------------------------------------------------------------
// getMount
//------------------------------------------------------------------------------
// NOT YET DECIDED
//cta::ArchiveMount &cta::ArchiveJob::getMount() {
//  return m_mount;
//}

//------------------------------------------------------------------------------
// getMount
//------------------------------------------------------------------------------
cta::ArchiveMount &cta::ArchiveJob::getMount() const {
  return m_mount;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveJob::getTapePoolName() const throw() {
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// getFileSize
//------------------------------------------------------------------------------
uint64_t cta::ArchiveJob::getFileSize() const throw() {
  return m_fileSize;
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::ArchiveJob::complete(const uint32_t checksumOfTransfer,
    const uint64_t fileSizeOfTransfer) {
}
  
//------------------------------------------------------------------------------
// failed
//------------------------------------------------------------------------------
void cta::ArchiveJob::failed(const std::exception &ex) {
}
  
//------------------------------------------------------------------------------
// retry
//------------------------------------------------------------------------------
void cta::ArchiveJob::retry() {
}
