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

#include "cta/FileSystemDirEntry.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirEntry::FileSystemDirEntry():
  m_storageClasses(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirEntry::FileSystemDirEntry(
  FileSystemStorageClasses &storageClasses, const DirEntry &entry):
  m_storageClasses(&storageClasses),
  m_entry(entry) {
  setStorageClassName(entry.getStorageClassName());
}

//------------------------------------------------------------------------------
// getEntry
//------------------------------------------------------------------------------
const cta::DirEntry &cta::FileSystemDirEntry::getEntry()
  const throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::FileSystemDirEntry::setStorageClassName(
  const std::string &storageClassName) {
  const std::string previousStorageClassName = m_entry.getStorageClassName();

  // If there is a change in storage class
  if(previousStorageClassName != storageClassName) {
    if(m_storageClasses) {
      m_storageClasses->decStorageClassUsageCount(previousStorageClassName);
      m_storageClasses->incStorageClassUsageCount(storageClassName);
    }

    m_entry.setStorageClassName(storageClassName);
  }
}
