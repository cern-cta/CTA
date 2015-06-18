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

#include "scheduler/FileStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileStatus::FileStatus():
  m_ownerId(0),
  m_groupId(0),
  m_mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileStatus::FileStatus(const std::string &storageClassName):
  m_ownerId(0),
  m_groupId(0),
  m_mode(0777),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getUid
//------------------------------------------------------------------------------
uint32_t cta::FileStatus::getUid() const throw() {
  return m_ownerId;
}

//------------------------------------------------------------------------------
// getGid
//------------------------------------------------------------------------------
uint32_t cta::FileStatus::getGid() const throw() {
  return m_groupId;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
mode_t cta::FileStatus::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::FileStatus::setStorageClassName(
  const std::string &storageClassName) {
  m_storageClassName = storageClassName;
}
  
//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::FileStatus::getStorageClassName() const throw() {
  return m_storageClassName;
}
