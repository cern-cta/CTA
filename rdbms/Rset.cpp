/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "rdbms/InvalidResultSet.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/Rset.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

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
Rset::Rset(std::unique_ptr<wrapper::RsetWrapper> impl):
  m_impl(std::move(impl)) {
  if(nullptr == m_impl.get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Pointer to implementation object is null");
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset(Rset &&other):
  m_impl(std::move(other.m_impl)) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Rset::~Rset() noexcept {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void Rset::reset() noexcept {
  m_impl.reset();
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
Rset &Rset::operator=(Rset &&rhs) {
  if(m_impl != rhs.m_impl) {
    m_impl = std::move(rhs.m_impl);
  }
  return *this;
}

//------------------------------------------------------------------------------
// columnString
//------------------------------------------------------------------------------
std::string Rset::columnBlob(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnBlob(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnString
//------------------------------------------------------------------------------
std::string Rset::columnString(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<std::string> col = columnOptionalString(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint8
//------------------------------------------------------------------------------
uint8_t Rset::columnUint8(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<uint8_t> col = columnOptionalUint8(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint16
//------------------------------------------------------------------------------
uint16_t Rset::columnUint16(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<uint16_t> col = columnOptionalUint16(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint32
//------------------------------------------------------------------------------
uint32_t Rset::columnUint32(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<uint32_t> col = columnOptionalUint32(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t Rset::columnUint64(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<uint64_t> col = columnOptionalUint64(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnBool
//------------------------------------------------------------------------------
bool Rset::columnBool(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<bool> col = columnOptionalBool(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalBool
//------------------------------------------------------------------------------
optional<bool> Rset::columnOptionalBool(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const auto column = columnOptionalUint64(colName);
    if(column) {
      return optional<bool>(column.value() != 0 ? true : false);
    } else {
      return nullopt;
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &Rset::getSql() const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->getSql();
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool Rset::next() {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const bool aRowHasBeenRetrieved = m_impl->next();

    // Release resources of result set when its end has been reached
    if(!aRowHasBeenRetrieved) {
      m_impl.reset(nullptr);
    }

    return aRowHasBeenRetrieved;
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isEmpty
//------------------------------------------------------------------------------
bool Rset::isEmpty() const
{
  return nullptr == m_impl;
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool Rset::columnIsNull(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnIsNull(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
optional<std::string> Rset::columnOptionalString(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalString(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint8
//------------------------------------------------------------------------------
optional<uint8_t> Rset::columnOptionalUint8(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalUint8(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint16
//------------------------------------------------------------------------------
optional<uint16_t> Rset::columnOptionalUint16(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalUint16(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint32
//------------------------------------------------------------------------------
optional<uint32_t> Rset::columnOptionalUint32(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalUint32(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
optional<uint64_t> Rset::columnOptionalUint64(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalUint64(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnDouble
//------------------------------------------------------------------------------
double Rset::columnDouble(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const optional<double> col = columnOptionalDouble(colName);
    if(col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalDouble
//------------------------------------------------------------------------------
optional<double> Rset::columnOptionalDouble(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->columnOptionalDouble(colName);
  } catch(exception::Exception &ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

} // namespace rdbms
} // namespace cta
