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

#include "common/ArchiveDirEntry.hpp"

//------------------------------------------------------------------------------
// entryTypeToStr
//------------------------------------------------------------------------------
const char *cta::ArchiveDirEntry::entryTypeToStr(const EntryType enumValue)
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
cta::ArchiveDirEntry::ArchiveDirEntry() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveDirEntry::ArchiveDirEntry(
  const EntryType entryType,
  const std::string &name,
  const ArchiveFileStatus &status):
  m_entryType(entryType),
  m_name(name),
  m_status(status) {
}

//------------------------------------------------------------------------------
// getType
//------------------------------------------------------------------------------
cta::ArchiveDirEntry::EntryType cta::ArchiveDirEntry::getType()
  const throw() {
  return m_entryType;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveDirEntry::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getStatus
//------------------------------------------------------------------------------
const cta::ArchiveFileStatus &cta::ArchiveDirEntry::getStatus() const throw() {
  return m_status;
}

//------------------------------------------------------------------------------
// getStatus
//------------------------------------------------------------------------------
cta::ArchiveFileStatus &cta::ArchiveDirEntry::getStatus() throw() {
  return m_status;
}
