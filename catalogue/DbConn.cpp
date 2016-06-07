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

#include "catalogue/DbConn.hpp"
#include "common/exception/Exception.hpp"

#include <memory>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
DbConn::~DbConn() throw() {
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void DbConn::executeNonQuery(const std::string &sql) {
  try {
    std::unique_ptr<DbStmt> stmt(createStmt(sql));
    stmt->executeNonQuery();
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.what());
  }
}

} // namespace catalogue
} // namespace cta
