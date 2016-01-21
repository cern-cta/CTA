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
#include "common/archiveNS/TapeFileLocation.hpp"

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
  const cta::ArchiveFile &archiveFile,
  const std::list<cta::TapeFileLocation> &tapeCopies,
  const std::string &remoteFile,
  const uint64_t priority,
  const CreationLog & creationLog):
  UserRetrieveRequest(priority, creationLog),
  m_archiveFile(archiveFile),
  m_tapeCopies(tapeCopies),
  m_remoteFile(remoteFile){
}

//------------------------------------------------------------------------------
// getArchiveFile
//------------------------------------------------------------------------------
const cta::ArchiveFile &cta::RetrieveToFileRequest::getArchiveFile() const throw() {
  return m_archiveFile;
}

//------------------------------------------------------------------------------
// getTapeCopies
//------------------------------------------------------------------------------
const std::list<cta::TapeFileLocation> &cta::RetrieveToFileRequest::
  getTapeCopies() const throw() {
  return m_tapeCopies;
}

//------------------------------------------------------------------------------
// getRemoteFile
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToFileRequest::getRemoteFile() const throw() {
  return m_remoteFile;
}
