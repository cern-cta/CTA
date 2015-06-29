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

#include "common/RemoteFileStatus.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemoteFileStatus::RemoteFileStatus():
  m_mode(0),
  m_size(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RemoteFileStatus::RemoteFileStatus(
  const UserIdentity &owner,
  const mode_t mode,
  const uint64_t size):
  m_owner(owner),
  m_mode(mode),
  m_size(size) {
}

//------------------------------------------------------------------------------
// getOwner
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::RemoteFileStatus::getOwner() const throw() {
  return m_owner;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
mode_t cta::RemoteFileStatus::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// getSize
//------------------------------------------------------------------------------
uint64_t cta::RemoteFileStatus::getSize() const throw() {
  return m_size;
}
