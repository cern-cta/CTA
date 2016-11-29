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
#include "rdbms/Conn.hpp"

#include <string>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Conn::~Conn() throw() {
}

//------------------------------------------------------------------------------
// executeNonQueries
//------------------------------------------------------------------------------
void Conn::executeNonQueries(const std::string &sqlStmts) {
  try {
    std::string::size_type searchPos = 0;
    std::string::size_type findResult = std::string::npos;

    while(std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
      const std::string::size_type length = findResult - searchPos + 1;
      const std::string sqlStmt = sqlStmts.substr(searchPos, length);
      searchPos = findResult + 1;
      executeNonQuery(sqlStmt, rdbms::Stmt::AutocommitMode::ON);
    }

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void Conn::executeNonQuery(const std::string &sql, const Stmt::AutocommitMode autocommitMode) {
  try {
    auto stmt = createStmt(sql, autocommitMode);
    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.what());
  }
}

} // namespace rdbms
} // namespace cta
