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

#include "cta/UserRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest():
  m_id(0),
  m_priority(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserRequest::UserRequest(
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  m_id(id),
  m_priority(priority),
  m_user(user),
  m_creationTime(creationTime) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::UserRequest::~UserRequest() throw() {
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
const std::string &cta::UserRequest::getId() const throw() {
  return m_id;
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
const cta::SecurityIdentity &cta::UserRequest::getUser() const throw() {
  return m_user;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::UserRequest::getCreationTime() const throw() {
  return m_creationTime;
}
