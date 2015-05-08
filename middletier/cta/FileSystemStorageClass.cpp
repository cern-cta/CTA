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

#define __STDC_LIMIT_MACROS

#include "cta/FileSystemStorageClass.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemStorageClass::FileSystemStorageClass():
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemStorageClass::FileSystemStorageClass(
  const StorageClass &storageClass):
  m_storageClass(storageClass),
  m_usageCount(0) {
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
const cta::StorageClass &cta::FileSystemStorageClass::getStorageClass()
  const throw() {
  return m_storageClass;
}

//------------------------------------------------------------------------------
// getUsageCount
//------------------------------------------------------------------------------
uint64_t cta::FileSystemStorageClass::getUsageCount() const throw() {
  return m_usageCount;
}

//------------------------------------------------------------------------------
// incUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClass::incUsageCount() {
  if(UINT64_MAX == m_usageCount) {
    std::ostringstream message;
    message << "Cannot increment usage count of storage class " <<
      m_storageClass.getName() << " because its maximum value of UINT64_MAX"
      " has already been reached";
    throw Exception(message.str());
  }
  m_usageCount++;
}

//------------------------------------------------------------------------------
// decUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemStorageClass::decUsageCount() {
  if(0 == m_usageCount) {
    std::ostringstream message;
    message << "Cannot decrement usage count of storage class " <<
      m_storageClass.getName() << " because it is already at zero";
    throw Exception(message.str());
  }
  m_usageCount--;
}
