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

#include "scheduler/ArchiveToFileRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::ArchiveToFileRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::~ArchiveToFileRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveToFileRequest::ArchiveToFileRequest(
  const RemotePathAndStatus &remoteFile,
  const std::string &archiveFile,
  const std::map<uint16_t, std::string> &copyNbToPoolMap,
  const uint64_t priority,
  const CreationLog & creationLog):
  ArchiveRequest(priority, creationLog),
  remoteFile(remoteFile),
  archiveFile(archiveFile),
  copyNbToPoolMap(copyNbToPoolMap) {
}
