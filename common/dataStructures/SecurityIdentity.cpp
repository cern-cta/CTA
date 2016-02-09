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

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::SecurityIdentity::SecurityIdentity() {  
  m_gidSet = false;
  m_hostSet = false;
  m_uidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::SecurityIdentity::~SecurityIdentity() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::SecurityIdentity::allFieldsSet() const {
  return m_gidSet
      && m_hostSet
      && m_uidSet;
}

//------------------------------------------------------------------------------
// setGid
//------------------------------------------------------------------------------
void cta::dataStructures::SecurityIdentity::setGid(const gid_t &gid) {
  m_gid = gid;
  m_gidSet = true;
}

//------------------------------------------------------------------------------
// getGid
//------------------------------------------------------------------------------
gid_t cta::dataStructures::SecurityIdentity::getGid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the SecurityIdentity have been set!");
  }
  return m_gid;
}

//------------------------------------------------------------------------------
// setHost
//------------------------------------------------------------------------------
void cta::dataStructures::SecurityIdentity::setHost(const std::string &host) {
  m_host = host;
  m_hostSet = true;
}

//------------------------------------------------------------------------------
// getHost
//------------------------------------------------------------------------------
std::string cta::dataStructures::SecurityIdentity::getHost() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the SecurityIdentity have been set!");
  }
  return m_host;
}

//------------------------------------------------------------------------------
// setUid
//------------------------------------------------------------------------------
void cta::dataStructures::SecurityIdentity::setUid(const uid_t &uid) {
  m_uid = uid;
  m_uidSet = true;
}

//------------------------------------------------------------------------------
// getUid
//------------------------------------------------------------------------------
uid_t cta::dataStructures::SecurityIdentity::getUid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the SecurityIdentity have been set!");
  }
  return m_uid;
}
