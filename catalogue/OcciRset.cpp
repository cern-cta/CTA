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

// Version 12.1 of oracle instant client uses the pre _GLIBCXX_USE_CXX11_ABI
#define _GLIBCXX_USE_CXX11_ABI 0

#include "catalogue/OcciRset.hpp"
#include "catalogue/OcciStmt.hpp"

#include <cstring>
#include <stdexcept>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciRset::OcciRset(OcciStmt &stmt, oracle::occi::ResultSet *const rset):
  m_stmt(stmt),
  m_rset(rset) {
  using namespace oracle;

  if(NULL == rset) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: rset is NULL");
  }

  const std::vector<occi::MetaData> columns = rset->getColumnListMetaData();
  for(unsigned int i = 0; i < columns.size(); i++) {
    // Column indices start at 1
    const unsigned int colIdx = i + 1;
    m_colNameToIdx.add(columns[i].getString(occi::MetaData::ATTR_NAME), colIdx);
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciRset::~OcciRset() throw() {
  try {
    close(); // Idempotent close()
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciRset::close() {
  std::lock_guard<std::mutex> lock(m_mutex);

  if(NULL != m_rset) {
    m_stmt->closeResultSet(m_rset);
    m_rset = NULL;
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
oracle::occi::ResultSet *OcciRset::get() const {
  return m_rset;
}

//------------------------------------------------------------------------------
// operator->
//------------------------------------------------------------------------------
oracle::occi::ResultSet *OcciRset::operator->() const {
  return get();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool OcciRset::next() {
  using namespace oracle;

  try {
    const occi::ResultSet::Status status = m_rset->next();
    return occi::ResultSet::DATA_AVAILABLE == status;
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

//------------------------------------------------------------------------------
// columnText
//------------------------------------------------------------------------------
char * OcciRset::columnText(const char *const colName) const {
  try {
    const int colIdx = m_colNameToIdx[colName];
    const std::string text = m_rset->getString(colIdx).c_str();
    char *const str = new char[text.length() + 1];
    std::memcpy(str, text.c_str(), text.length());
    str[text.length()] = '\0';
    return str;
  } catch(std::exception &ne) {
    throw std::runtime_error(std::string(__FUNCTION__) + " failed: " + ne.what());
  }
}

} // namespace catalogue
} // namespace cta
