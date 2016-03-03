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

#include "common/dataStructures/EntryLog.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog::EntryLog() {  
  m_hostSet = false;
  m_timeSet = false;
  m_userSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog::~EntryLog() throw() {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::EntryLog::operator==(const EntryLog &rhs)
  const {
  if(!allFieldsSet()) {
    throw exception::Exception(
      "Not all of fields of EntryLog on LHS of == are set");
  }
  if(!rhs.allFieldsSet()) {
    throw exception::Exception(
      "Not all of fields of EntryLog on RHS of == are set"); 
  }
  return m_host == rhs.m_host && m_time == rhs.m_time && m_user == rhs.m_user;
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::EntryLog::allFieldsSet() const {
  return m_hostSet
      && m_timeSet
      && m_userSet;
}

//------------------------------------------------------------------------------
// setHost
//------------------------------------------------------------------------------
void cta::common::dataStructures::EntryLog::setHost(const std::string &host) {
  m_host = host;
  m_hostSet = true;
}

//------------------------------------------------------------------------------
// getHost
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::EntryLog::getHost() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the EntryLog have been set!");
  }
  return m_host;
}

//------------------------------------------------------------------------------
// setTime
//------------------------------------------------------------------------------
void cta::common::dataStructures::EntryLog::setTime(const time_t &time) {
  m_time = time;
  m_timeSet = true;
}

//------------------------------------------------------------------------------
// getTime
//------------------------------------------------------------------------------
time_t cta::common::dataStructures::EntryLog::getTime() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the EntryLog have been set!");
  }
  return m_time;
}

//------------------------------------------------------------------------------
// setUser
//------------------------------------------------------------------------------
void cta::common::dataStructures::EntryLog::setUser(const cta::common::dataStructures::UserIdentity &user) {
  m_user = user;
  m_userSet = true;
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity cta::common::dataStructures::EntryLog::getUser() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the EntryLog have been set!");
  }
  return m_user;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &cta::common::dataStructures::operator<<(std::ostream &os,
  const cta::common::dataStructures::EntryLog &entryLog) {
  if(!entryLog.allFieldsSet()) {
    throw exception::Exception(
      "operator<<( for EntryLog failed: Not all fields are set");
  }
  os << "{"
    "host=" << entryLog.getHost() << " " <<
    "time=" << entryLog.getTime() << " " <<
    "user=" << entryLog.getUser() << "}";
  return os;
}
