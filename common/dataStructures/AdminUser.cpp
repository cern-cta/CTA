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

#include "common/dataStructures/AdminUser.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::AdminUser::AdminUser() {  
  m_commentSet = false;
  m_creationLogSet = false;
  m_lastModificationLogSet = false;
  m_userSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::AdminUser::~AdminUser() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::AdminUser::allFieldsSet() const {
  return m_commentSet
      && m_creationLogSet
      && m_lastModificationLogSet
      && m_userSet;
}

//------------------------------------------------------------------------------
// setComment
//------------------------------------------------------------------------------
void cta::dataStructures::AdminUser::setComment(const std::string &comment) {
  m_comment = comment;
  m_commentSet = true;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
std::string cta::dataStructures::AdminUser::getComment() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the AdminUser have been set!");
  }
  return m_comment;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::dataStructures::AdminUser::setCreationLog(const cta::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::AdminUser::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the AdminUser have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setLastModificationLog
//------------------------------------------------------------------------------
void cta::dataStructures::AdminUser::setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog) {
  m_lastModificationLog = lastModificationLog;
  m_lastModificationLogSet = true;
}

//------------------------------------------------------------------------------
// getLastModificationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::AdminUser::getLastModificationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the AdminUser have been set!");
  }
  return m_lastModificationLog;
}

//------------------------------------------------------------------------------
// setUser
//------------------------------------------------------------------------------
void cta::dataStructures::AdminUser::setUser(const cta::dataStructures::UserIdentity &user) {
  m_user = user;
  m_userSet = true;
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
cta::dataStructures::UserIdentity cta::dataStructures::AdminUser::getUser() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the AdminUser have been set!");
  }
  return m_user;
}
