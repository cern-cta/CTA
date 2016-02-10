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
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UserIdentity::allFieldsSet() const {
  return m_gidSet
      && m_uidSet;
}

//------------------------------------------------------------------------------
// setGid
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setGid(const uint32_t gid) {
  m_gid = gid;
  m_gidSet = true;
}

//------------------------------------------------------------------------------
// getGid
//------------------------------------------------------------------------------
uint32_t cta::common::dataStructures::UserIdentity::getGid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_gid;
}

//------------------------------------------------------------------------------
// setUid
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setUid(const uint32_t uid) {
  m_uid = uid;
  m_uidSet = true;
}

//------------------------------------------------------------------------------
// getUid
//------------------------------------------------------------------------------
uint32_t cta::common::dataStructures::UserIdentity::getUid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_uid;
}
