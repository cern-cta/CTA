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

#include "scheduler/RetrieveToFileRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::RetrieveToFileRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::~RetrieveToFileRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToFileRequest::RetrieveToFileRequest(
  const std::string &archiveFile,
  const std::string &remoteFile,
  const std::string &id, 
  const uint64_t priority,
  const SecurityIdentity &user, 
  const time_t creationTime):
  RetrievalRequest(id, priority, user, creationTime),
  m_archiveFile(archiveFile),
  m_remoteFile(remoteFile) {
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToFileRequest::getArchiveFile() const throw() {
  return m_archiveFile;
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToFileRequest::getRemoteFile() const throw() {
  return m_remoteFile;
}
