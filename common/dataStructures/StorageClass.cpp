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

#include "common/dataStructures/StorageClass.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::StorageClass::StorageClass() {  
  m_commentSet = false;
  m_creationLogSet = false;
  m_lastModificationLogSet = false;
  m_nameSet = false;
  m_nbCopiesSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::StorageClass::~StorageClass() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::StorageClass::allFieldsSet() const {
  return m_commentSet
      && m_creationLogSet
      && m_lastModificationLogSet
      && m_nameSet
      && m_nbCopiesSet;
}

//------------------------------------------------------------------------------
// setComment
//------------------------------------------------------------------------------
void cta::dataStructures::StorageClass::setComment(const std::string &comment) {
  m_comment = comment;
  m_commentSet = true;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
std::string cta::dataStructures::StorageClass::getComment() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the StorageClass have been set!");
  }
  return m_comment;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::dataStructures::StorageClass::setCreationLog(const cta::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::StorageClass::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the StorageClass have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setLastModificationLog
//------------------------------------------------------------------------------
void cta::dataStructures::StorageClass::setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog) {
  m_lastModificationLog = lastModificationLog;
  m_lastModificationLogSet = true;
}

//------------------------------------------------------------------------------
// getLastModificationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::StorageClass::getLastModificationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the StorageClass have been set!");
  }
  return m_lastModificationLog;
}

//------------------------------------------------------------------------------
// setName
//------------------------------------------------------------------------------
void cta::dataStructures::StorageClass::setName(const std::string &name) {
  m_name = name;
  m_nameSet = true;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string cta::dataStructures::StorageClass::getName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the StorageClass have been set!");
  }
  return m_name;
}

//------------------------------------------------------------------------------
// setNbCopies
//------------------------------------------------------------------------------
void cta::dataStructures::StorageClass::setNbCopies(const uint16_t nbCopies) {
  m_nbCopies = nbCopies;
  m_nbCopiesSet = true;
}

//------------------------------------------------------------------------------
// getNbCopies
//------------------------------------------------------------------------------
uint16_t cta::dataStructures::StorageClass::getNbCopies() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the StorageClass have been set!");
  }
  return m_nbCopies;
}
