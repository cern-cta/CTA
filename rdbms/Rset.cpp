/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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

    const std::optional<std::string> col = columnOptionalString(colName);
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

    const std::optional<uint8_t> col = columnOptionalUint8(colName);
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

    const std::optional<uint16_t> col = columnOptionalUint16(colName);
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

    const std::optional<uint32_t> col = columnOptionalUint32(colName);
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

    const std::optional<uint64_t> col = columnOptionalUint64(colName);
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

    const std::optional<bool> col = columnOptionalBool(colName);
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
std::optional<bool> Rset::columnOptionalBool(const std::string &colName) const {
  try {
    if(nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const auto column = columnOptionalUint64(colName);
    if(column) {
      return std::optional<bool>(column.value() != 0 ? true : false);
    } else {
      return std::nullopt;
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
std::optional<std::string> Rset::columnOptionalString(const std::string &colName) const {
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
std::optional<uint8_t> Rset::columnOptionalUint8(const std::string &colName) const {
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
std::optional<uint16_t> Rset::columnOptionalUint16(const std::string &colName) const {
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
std::optional<uint32_t> Rset::columnOptionalUint32(const std::string &colName) const {
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
std::optional<uint64_t> Rset::columnOptionalUint64(const std::string &colName) const {
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

    const std::optional<double> col = columnOptionalDouble(colName);
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
std::optional<double> Rset::columnOptionalDouble(const std::string &colName) const {
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
