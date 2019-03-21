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

#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "rdbms/wrapper/OcciConnFactory.hpp"
#include "rdbms/wrapper/OcciEnvSingleton.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciConnFactory::OcciConnFactory(
  const std::string &username,
  const std::string &password,
  const std::string &database):
  m_username(username),
  m_password(password),
  m_database(database) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciConnFactory::~OcciConnFactory() {
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<ConnWrapper> OcciConnFactory::create() {
  try {
    return OcciEnvSingleton::instance().createConn(m_username, m_password, m_database);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
