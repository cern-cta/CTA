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

#include "common/dataStructures/Requester.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::Requester::Requester() {  
  m_groupNameSet = false;
  m_userNameSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::Requester::~Requester() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Requester::allFieldsSet() const {
  return m_groupNameSet
      && m_userNameSet;
}

//------------------------------------------------------------------------------
// setGroupName
//------------------------------------------------------------------------------
void cta::common::dataStructures::Requester::setGroupName(const std::string &groupName) {
  m_groupName = groupName;
  m_groupNameSet = true;
}

//------------------------------------------------------------------------------
// getGroupName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Requester::getGroupName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Requester have been set!");
  }
  return m_groupName;
}

//------------------------------------------------------------------------------
// setUserName
//------------------------------------------------------------------------------
void cta::common::dataStructures::Requester::setUserName(const std::string &userName) {
  m_userName = userName;
  m_userNameSet = true;
}

//------------------------------------------------------------------------------
// getUserName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Requester::getUserName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Requester have been set!");
  }
  return m_userName;
}
