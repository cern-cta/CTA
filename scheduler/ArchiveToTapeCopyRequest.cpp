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

#include "scheduler/ArchiveToTapeCopyRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToTapeCopyRequest::ArchiveToTapeCopyRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveToTapeCopyRequest::~ArchiveToTapeCopyRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToTapeCopyRequest::ArchiveToTapeCopyRequest(
  const RemotePathAndStatus &remoteFile,
  const std::string &archiveFile,
  const uint16_t copyNb,
  const std::string tapePoolName,
  const uint64_t priority,
  const CreationLog &creationLog):
  ArchiveRequest(priority, creationLog),
  m_remoteFile(remoteFile),
  m_archiveFile(archiveFile),
  m_copyNb(copyNb),
  m_tapePoolName(tapePoolName) {
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const cta::RemotePathAndStatus &cta::ArchiveToTapeCopyRequest::getRemoteFile()
  const throw() {
  return m_remoteFile;
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
const std::string &cta::ArchiveToTapeCopyRequest::getArchiveFile() const throw() {
 return m_archiveFile;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint16_t cta::ArchiveToTapeCopyRequest::getCopyNb() const throw() {
  return m_copyNb;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveToTapeCopyRequest::getTapePoolName() const
  throw() {
  return m_tapePoolName;
}
