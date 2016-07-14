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

#include "OcciEnv.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace rdbms {

/**
 * Following the pimpl idiom, this is the class actually implementing OcciEnv.
 */
class OcciEnv::Impl {
public:

  /**
   * Throws an exception because OCCI support not enabled at compile time.
   */
  DbConn *createConn( const std::string &username, const std::string &password, const std::string &database) {
    throw exception::Exception(std::string(__FUNCTION__) + ": OCCI support not enabled at compile time");
  }

}; // class Impl

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnv::OcciEnv(): m_impl(new Impl()) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciEnv::~OcciEnv() = default;

//------------------------------------------------------------------------------
// createConn
//------------------------------------------------------------------------------
DbConn *OcciEnv::createConn(
  const std::string &username,
  const std::string &password,
  const std::string &database) {
  return m_impl->createConn(username, password, database);
}

} // namespace rdbms
} // namespace cta
