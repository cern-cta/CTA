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

#include "common/ArchiveFileStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveFileStatus::ArchiveFileStatus():
  m_mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveFileStatus::ArchiveFileStatus(
  const UserIdentity &owner,
  const mode_t mode,
  const std::string &storageClassName):
  m_owner(owner),
  m_mode(mode),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::ArchiveFileStatus::getOwner() const throw() {
  return m_owner;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
mode_t cta::ArchiveFileStatus::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::ArchiveFileStatus::setStorageClassName(
  const std::string &storageClassName) {
  m_storageClassName = storageClassName;
}
  
//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveFileStatus::getStorageClassName() const throw() {
  return m_storageClassName;
}
