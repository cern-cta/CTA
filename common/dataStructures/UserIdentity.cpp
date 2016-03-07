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
  m_groupSet = false;
  m_nameSet = false;
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
  return m_groupSet
      && m_nameSet;
}

//------------------------------------------------------------------------------
// setGroup
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setGroup(const std::string &group) {
  m_group = group;
  m_groupSet = true;
}

//------------------------------------------------------------------------------
// getGroup
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::UserIdentity::getGroup() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_group;
}

//------------------------------------------------------------------------------
// setName
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserIdentity::setName(const std::string &name) {
  m_name = name;
  m_nameSet = true;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::UserIdentity::getName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserIdentity have been set!");
  }
  return m_name;
}
