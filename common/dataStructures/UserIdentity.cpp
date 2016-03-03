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

#include "common/dataStructures/UserIdentity.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity::UserIdentity() {  
  m_gidSet = false;
  m_uidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::UserIdentity::~UserIdentity() throw() {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UserIdentity::operator==(
  const UserIdentity &rhs) const {
  if(!allFieldsSet()) {
    throw exception::Exception(
      "Not all of fields of UserIdentity on LHS of == are set");
  }
  if(!rhs.allFieldsSet()) {
    throw exception::Exception(
      "Not all of fields of UserIdentity on RHS of == are set");
  }
  return m_gid == rhs.m_gid && m_uidSet == rhs.m_uidSet;
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UserIdentity::allFieldsSet() const {
  return m_gidSet
      && m_uidSet;
}

//------------------------------------------------------------------------------
// setGid
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setGid(const uint64_t gid) {
  m_gid = gid;
  m_gidSet = true;
}

//------------------------------------------------------------------------------
// getGid
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserIdentity::getGid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_gid;
}

//------------------------------------------------------------------------------
// setUid
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setUid(const uint64_t uid) {
  m_uid = uid;
  m_uidSet = true;
}

//------------------------------------------------------------------------------
// getUid
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserIdentity::getUid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_uid;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &cta::common::dataStructures::operator<<(std::ostream &os,
  const UserIdentity &userIdentity) {
  if(!userIdentity.allFieldsSet()) {
    throw exception::Exception(
      "operator<< for UserIdentity failed: Not all fields are set");
  }
  os << "{gid=" << userIdentity.m_gid << " uid=" << userIdentity.m_uid << "}";
  return os;
}
