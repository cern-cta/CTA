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

#include "catalogue/OcciRset.hpp"
#include "catalogue/OcciStmt.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::OcciRset::OcciRset(OcciStmt &stmt,
  oracle::occi::ResultSet *const rset):
  m_stmt(stmt),
  m_rset(rset) {
  if(NULL == rset) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed"
      ": The result set is a NULL pointer";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::OcciRset::~OcciRset() throw() {
  try {
    close(); // Idempotenet close()
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void cta::catalogue::OcciRset::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(NULL != m_rset) {
    m_stmt->closeResultSet(m_rset);
    m_rset = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::ResultSet *cta::catalogue::OcciRset::get() const {
  return m_rset;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::ResultSet *cta::catalogue::OcciRset::operator->() const {
  return get();
}
