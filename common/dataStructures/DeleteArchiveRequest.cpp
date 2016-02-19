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

#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::DeleteArchiveRequest::DeleteArchiveRequest() {  
  m_archiveFileIDSet = false;
  m_requesterSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::DeleteArchiveRequest::~DeleteArchiveRequest() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::DeleteArchiveRequest::allFieldsSet() const {
  return m_archiveFileIDSet
      && m_requesterSet;
}

//------------------------------------------------------------------------------
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::common::dataStructures::DeleteArchiveRequest::setArchiveFileID(const uint64_t archiveFileID) {
  m_archiveFileID = archiveFileID;
  m_archiveFileIDSet = true;
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::DeleteArchiveRequest::getArchiveFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DeleteArchiveRequest have been set!");
  }
  return m_archiveFileID;
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void cta::common::dataStructures::DeleteArchiveRequest::setRequester(const cta::common::dataStructures::Requester &requester) {
  m_requester = requester;
  m_requesterSet = true;
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::common::dataStructures::Requester cta::common::dataStructures::DeleteArchiveRequest::getRequester() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DeleteArchiveRequest have been set!");
  }
  return m_requester;
}
