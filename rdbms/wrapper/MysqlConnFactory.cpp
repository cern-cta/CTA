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
#include "rdbms/wrapper/MysqlConn.hpp"
#include "rdbms/wrapper/MysqlConnFactory.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlConnFactory::MysqlConnFactory(const std::string& host, 
                                   const std::string& user, 
                                   const std::string& passwd,
                                   const std::string& db,
                                   unsigned int port):
  m_host(host), m_user(user), m_passwd(passwd), m_db(db), m_port(port) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
MysqlConnFactory::~MysqlConnFactory() {
}

//------------------------------------------------------------------------------
// create
//------------------------------------------------------------------------------
std::unique_ptr<Conn> MysqlConnFactory::create() {
  try {
    return cta::make_unique<MysqlConn>(m_host, m_user, m_passwd, m_db, m_port);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
