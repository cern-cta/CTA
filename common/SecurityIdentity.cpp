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

#include "common/SecurityIdentity.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SecurityIdentity::SecurityIdentity() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::SecurityIdentity::SecurityIdentity(const UserIdentity &user,
  const std::string &host): m_user(user), m_host(host) {
}

//------------------------------------------------------------------------------
// getUser
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::SecurityIdentity::getUser() const throw() {
  return m_user;
}

//------------------------------------------------------------------------------
// getHost
//------------------------------------------------------------------------------
const std::string &cta::SecurityIdentity::getHost() const throw() {
  return m_host;
}
