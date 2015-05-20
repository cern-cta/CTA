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

#include "scheduler/DirEntry.hpp"

//------------------------------------------------------------------------------
// entryTypeToStr
//------------------------------------------------------------------------------
const char *cta::DirEntry::entryTypeToStr(const EntryType enumValue)
  throw() {
  switch(enumValue) {
  case ENTRYTYPE_NONE     : return "NONE";
  case ENTRYTYPE_FILE     : return "FILE";
  case ENTRYTYPE_DIRECTORY: return "DIRECTORY";
  default                 : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirEntry::DirEntry():
  m_ownerId(0),
  m_groupId(0),
  m_mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirEntry::DirEntry(const EntryType entryType,
  const std::string &name, const std::string &storageClassName):
  m_entryType(entryType),
  m_name(name),
  m_ownerId(0),
  m_groupId(0),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getType
//------------------------------------------------------------------------------
cta::DirEntry::EntryType cta::DirEntry::getType()
  const throw() {
  return m_entryType;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::DirEntry::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getOwnerId
//------------------------------------------------------------------------------
uint32_t cta::DirEntry::getOwnerId() const throw() {
  return m_ownerId;
}

//------------------------------------------------------------------------------
// getGroupId
//------------------------------------------------------------------------------
uint32_t cta::DirEntry::getGroupId() const throw() {
  return m_groupId;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
uint16_t cta::DirEntry::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::DirEntry::setStorageClassName(
  const std::string &storageClassName) {
  m_storageClassName = storageClassName;
}
  
//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::DirEntry::getStorageClassName() const throw() {
  return m_storageClassName;
}
