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

#include "middletier/interface/UserIdentity.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity() throw():
  m_uid(0),
  m_gid(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::UserIdentity::UserIdentity(
  const uint32_t uid,
  const uint32_t gid) throw():
  m_uid(uid),
  m_gid(gid) {
}

//------------------------------------------------------------------------------
// setUid
//------------------------------------------------------------------------------
void cta::UserIdentity::setUid(const uint32_t uid) throw() {
  m_uid = uid;
} 

//------------------------------------------------------------------------------
// getUid
//------------------------------------------------------------------------------
uint32_t cta::UserIdentity::getUid() const throw() {
  return m_uid;
}

//------------------------------------------------------------------------------
// setGid
//------------------------------------------------------------------------------
void cta::UserIdentity::setGid(const uint32_t gid) throw() {
  m_gid = gid;
}

//------------------------------------------------------------------------------
// getGid
//------------------------------------------------------------------------------
uint32_t cta::UserIdentity::getGid() const throw() {
  return m_gid;
}
