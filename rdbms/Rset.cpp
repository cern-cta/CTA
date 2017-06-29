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

#include "Rset.hpp"
#include "RsetImpl.hpp"
#include "NullDbValue.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset():
  m_impl(nullptr) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset(RsetImpl *const impl):
  m_impl(impl) {
  if(nullptr == impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Pointer to implementation object is null");
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset(Rset &&other):
  m_impl(other.m_impl) {
  other.m_impl = nullptr;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Rset::~Rset() {
  delete m_impl;
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
Rset &Rset::operator=(Rset &&rhs) {
  m_impl = rhs.m_impl;
  rhs.m_impl = nullptr;
  return *this;
}

//------------------------------------------------------------------------------
// columnString
//------------------------------------------------------------------------------
std::string Rset::columnString(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw exception::Exception("This result set is invalid");
    }

    const optional<std::string> col = columnOptionalString(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t Rset::columnUint64(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw exception::Exception("This result set is invalid");
    }

    const optional<uint64_t> col = columnOptionalUint64(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// columnBool
//------------------------------------------------------------------------------
bool Rset::columnBool(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw exception::Exception("This result set is invalid");
    }

    const optional<bool> col = columnOptionalBool(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// columnOptionalBool
//------------------------------------------------------------------------------
optional<bool> Rset::columnOptionalBool(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw exception::Exception("This result set is invalid");
    }

    const auto column = columnOptionalUint64(colName);
    if(column) {
      return optional<bool>(column.value() != 0 ? true : false);
    } else {
      return nullopt;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &Rset::getSql() const {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This result set is invalid");
  }
  return m_impl->getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool Rset::next() {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This result set is invalid");
  }
  return m_impl->next();
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool Rset::columnIsNull(const std::string &colName) const {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This result set is invalid");
  }
  return m_impl->columnIsNull(colName);
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
optional<std::string> Rset::columnOptionalString(const std::string &colName) const {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This result set is invalid");
  }
  return m_impl->columnOptionalString(colName);
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
optional<uint64_t> Rset::columnOptionalUint64(const std::string &colName) const {
  if(nullptr == m_impl) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: "
      "This result set is invalid");
  }
  return m_impl->columnOptionalUint64(colName);
}

} // namespace rdbms
} // namespace cta
