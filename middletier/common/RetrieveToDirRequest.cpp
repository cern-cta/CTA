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

#include "middletier/common/RetrieveToDirRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::RetrieveToDirRquest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::~RetrieveToDirRquest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveToDirRquest::RetrieveToDirRquest(
  const std::string &remoteDir,
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  RetrievalRequest(id, priority, user, creationTime),
  m_remoteDir(remoteDir) {
}

//------------------------------------------------------------------------------
// getRemoteDir
//------------------------------------------------------------------------------
const std::string &cta::RetrieveToDirRquest::getRemoteDir() const throw() {
  return m_remoteDir;
}

//------------------------------------------------------------------------------
// getRetrieveToFileRequests
//------------------------------------------------------------------------------
const std::list<cta::RetrieveToFileRequest> &cta::RetrieveToDirRquest::
  getRetrieveToFileRequests() const throw() {
  return m_retrieveToFileRequests;
}

//------------------------------------------------------------------------------
// getRetrieveToFileRequests
//------------------------------------------------------------------------------
std::list<cta::RetrieveToFileRequest> &cta::RetrieveToDirRquest::
  getRetrieveToFileRequests() throw() {
  return m_retrieveToFileRequests;
}
