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

#include "scheduler/UserRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest():
  m_priority(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest(
  const uint64_t priority,
  const SecurityIdentity &requester,
  const time_t creationTime):
  m_priority(priority),
  m_requester(requester),
  m_creationTime(creationTime) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::UserRequest::~UserRequest() throw() {
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
const cta::SecurityIdentity &cta::UserRequest::getRequester() const throw() {
  return m_requester;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::UserRequest::getCreationTime() const throw() {
  return m_creationTime;
}
