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

#include "rdbms/NullDbValue.hpp"
#include "rdbms/Rset.hpp"
#include "rdbms/wrapper/RsetWrapper.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset() : m_impl(nullptr) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset(std::unique_ptr<wrapper::RsetWrapper> impl) : m_impl(std::move(impl)) {
  if (nullptr == m_impl.get()) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Pointer to implementation object is null");
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Rset::Rset(Rset&& other) : m_impl(std::move(other.m_impl)) {}

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
Rset& Rset::operator=(Rset&& rhs) {
  if (m_impl != rhs.m_impl) {
    m_impl = std::move(rhs.m_impl);
  }
  return *this;
}

//------------------------------------------------------------------------------
// Implementation of methods getting column values from string memory buffer of postgres
// without passing through optionals, then throw Null case exception within the implementation (downside)
// but significantly improve the performance (20x)
//------------------------------------------------------------------------------

uint8_t Rset::columnUint8NoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnUint8NoOpt, colName);
}

uint16_t Rset::columnUint16NoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnUint16NoOpt, colName);
}

uint32_t Rset::columnUint32NoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnUint32NoOpt, colName);
}

uint64_t Rset::columnUint64NoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnUint64NoOpt, colName);
}

std::string Rset::columnStringNoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnStringNoOpt, colName);
}

double Rset::columnDoubleNoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnDoubleNoOpt, colName);
}

bool Rset::columnBoolNoOpt(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnBoolNoOpt, colName);
}

//------------------------------------------------------------------------------
// columnString
//------------------------------------------------------------------------------
std::string Rset::columnBlob(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnBlob, colName);
}

//------------------------------------------------------------------------------
// columnString
//------------------------------------------------------------------------------
std::string Rset::columnString(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<std::string> col = columnOptionalString(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint8
//------------------------------------------------------------------------------
uint8_t Rset::columnUint8(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<uint8_t> col = columnOptionalUint8(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint16
//------------------------------------------------------------------------------
uint16_t Rset::columnUint16(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<uint16_t> col = columnOptionalUint16(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint32
//------------------------------------------------------------------------------
uint32_t Rset::columnUint32(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<uint32_t> col = columnOptionalUint32(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnUint64
//------------------------------------------------------------------------------
uint64_t Rset::columnUint64(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<uint64_t> col = columnOptionalUint64(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnBool
//------------------------------------------------------------------------------
bool Rset::columnBool(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<bool> col = columnOptionalBool(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalBool
//------------------------------------------------------------------------------
std::optional<bool> Rset::columnOptionalBool(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    // Attempt to get the column as a uint64
    try {
      const auto column = columnOptionalUint64(colName);
      if (column) {
        return std::optional<bool>(column.value() != 0 ? true : false);
      } else {
        return std::nullopt;
      }
    } catch (exception::Exception& ex) {
      ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
      // ignoring this exception and continue looking for string 'f' or 't'
      // the following covers cases when we would use BOOLEAN in the schema
      const auto column = columnOptionalString(colName);
      if (column) {
        const std::string& strValue = column.value();
        if (strValue == "t" || strValue == "true") {
          return std::optional<bool>(true);
        } else if (strValue == "f" || strValue == "false") {
          return std::optional<bool>(false);
        } else {
          throw exception::Exception("Invalid boolean string representation: " + strValue);
        }
      } else {
        return std::nullopt;
      }
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string& Rset::getSql() const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }
    return m_impl->getSql();
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool Rset::next() {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const bool aRowHasBeenRetrieved = m_impl->next();

    // Release resources of result set when its end has been reached
    if (!aRowHasBeenRetrieved) {
      m_impl.reset(nullptr);
    }

    return aRowHasBeenRetrieved;
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// isEmpty
//------------------------------------------------------------------------------
bool Rset::isEmpty() const {
  return nullptr == m_impl;
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool Rset::columnIsNull(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnIsNull, colName);
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
std::optional<std::string> Rset::columnOptionalString(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalString, colName);
}

//------------------------------------------------------------------------------
// columnOptionalUint8
//------------------------------------------------------------------------------
std::optional<uint8_t> Rset::columnOptionalUint8(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalUint8, colName);
}

//------------------------------------------------------------------------------
// columnOptionalUint16
//------------------------------------------------------------------------------
std::optional<uint16_t> Rset::columnOptionalUint16(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalUint16, colName);
}

//------------------------------------------------------------------------------
// columnOptionalUint32
//------------------------------------------------------------------------------
std::optional<uint32_t> Rset::columnOptionalUint32(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalUint32, colName);
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
std::optional<uint64_t> Rset::columnOptionalUint64(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalUint64, colName);
}

//------------------------------------------------------------------------------
// columnDouble
//------------------------------------------------------------------------------
double Rset::columnDouble(const std::string& colName) const {
  try {
    if (nullptr == m_impl) {
      throw InvalidResultSet("This result set is invalid");
    }

    const std::optional<double> col = columnOptionalDouble(colName);
    if (col) {
      return col.value();
    } else {
      throw NullDbValue(std::string("Database column ") + colName + " contains a null value");
    }
  } catch (exception::Exception& ex) {
    ex.getMessage().str(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
    throw;
  }
}

//------------------------------------------------------------------------------
// columnOptionalDouble
//------------------------------------------------------------------------------
std::optional<double> Rset::columnOptionalDouble(const std::string& colName) const {
  return delegateToImpl(&wrapper::RsetWrapper::columnOptionalDouble, colName);
}

}  // namespace cta::rdbms
