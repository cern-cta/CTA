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
#include "common/utils/utils.hpp"
#include "rdbms/NullDbValue.hpp"
#include "rdbms/wrapper/OcciRset.hpp"
#include "rdbms/wrapper/OcciStmt.hpp"

#include <cstring>
#include <map>
#include <stdexcept>

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciRset::OcciRset(OcciStmt &stmt, oracle::occi::ResultSet *const rset):
  m_stmt(stmt),
  m_rset(rset) {
  try {
    if (nullptr == rset) {
      throw exception::Exception("rset is nullptr");
    }
    populateColNameToIdxMap();
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// populateColNameToIdx
//------------------------------------------------------------------------------
void OcciRset::populateColNameToIdxMap() {
  using namespace oracle;

  try {
    const std::vector<occi::MetaData> columns = m_rset->getColumnListMetaData();
    for (unsigned int i = 0; i < columns.size(); i++) {
      // Column indices start at 1
      const unsigned int colIdx = i + 1;
      m_colNameToIdx.add(columns[i].getString(occi::MetaData::ATTR_NAME), colIdx);
    }
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
OcciRset::~OcciRset() {
  try {
    close(); // Idempotent close()
  } catch(...) {
    // Destructor does not throw
  }
}

//------------------------------------------------------------------------------
// getSql
//------------------------------------------------------------------------------
const std::string &OcciRset::getSql() const {
  return m_stmt.getSql();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
bool OcciRset::next() {
  using namespace oracle;

  try {
    const occi::ResultSet::Status status = m_rset->next();
    return occi::ResultSet::DATA_AVAILABLE == status;
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      se.what());
  }
}

//------------------------------------------------------------------------------
// columnIsNull
//------------------------------------------------------------------------------
bool OcciRset::columnIsNull(const std::string &colName) const {
  try {
    const int colIdx = m_colNameToIdx.getIdx(colName);
    return m_rset->isNull(colIdx);
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void OcciRset::close() {
  threading::Mutex locker(m_mutex);

  if(nullptr != m_rset) {
    m_stmt->closeResultSet(m_rset);
    m_rset = nullptr;
  }
}

std::string OcciRset::columnBlob(const std::string &colName) const {
  try {
    const int colIdx = m_colNameToIdx.getIdx(colName);
    auto raw = m_rset->getBytes(colIdx);
    std::unique_ptr<unsigned char[]> bytearray(new unsigned char[raw.length()]());
    raw.getBytes(bytearray.get(), raw.length());
    return std::string(reinterpret_cast<char*>(bytearray.get()), raw.length());
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      se.what());
  }
}

//------------------------------------------------------------------------------
// columnOptionalString
//------------------------------------------------------------------------------
optional<std::string> OcciRset::columnOptionalString(const std::string &colName) const {
  try {
    const int colIdx = m_colNameToIdx.getIdx(colName);
    const std::string stringValue = m_rset->getString(colIdx);
    if(stringValue.empty()) {
      return nullopt;
    }
    return stringValue;
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      se.what());
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint16
//------------------------------------------------------------------------------
optional<uint16_t> OcciRset::columnOptionalUint16(const std::string &colName) const {
  try {
    threading::Mutex locker(m_mutex);

    const int colIdx = m_colNameToIdx.getIdx(colName);
    const std::string stringValue = m_rset->getString(colIdx);
    if(stringValue.empty()) {
      return nullopt;
    }
    if(!utils::isValidUInt(stringValue)) {
      throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                                 " which is not a valid unsigned integer");
    }
    return utils::toUint16(stringValue);
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
                               ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
                               se.what());
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint32
//------------------------------------------------------------------------------
optional<uint32_t> OcciRset::columnOptionalUint32(const std::string &colName) const {
  try {
    threading::Mutex locker(m_mutex);

    const int colIdx = m_colNameToIdx.getIdx(colName);
    const std::string stringValue = m_rset->getString(colIdx);
    if(stringValue.empty()) {
      return nullopt;
    }
    if(!utils::isValidUInt(stringValue)) {
      throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
                                 " which is not a valid unsigned integer");
    }
    return utils::toUint32(stringValue);
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
                               ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
                               se.what());
  }
}

//------------------------------------------------------------------------------
// columnOptionalUint64
//------------------------------------------------------------------------------
optional<uint64_t> OcciRset::columnOptionalUint64(const std::string &colName) const {
  try {
    threading::Mutex locker(m_mutex);

    const int colIdx = m_colNameToIdx.getIdx(colName);
    const std::string stringValue = m_rset->getString(colIdx);
    if(stringValue.empty()) {
      return nullopt;
    }
    if(!utils::isValidUInt(stringValue)) {
      throw exception::Exception(std::string("Column ") + colName + " contains the value " + stringValue +
        " which is not a valid unsigned integer");
    }
    return utils::toUint64(stringValue);
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      se.what());
  }
}

//------------------------------------------------------------------------------
// columnOptionalDouble
//------------------------------------------------------------------------------
optional<double> OcciRset::columnOptionalDouble(const std::string &colName) const {
  try {
    threading::Mutex locker(m_mutex);

    const int colIdx = m_colNameToIdx.getIdx(colName);
    if(m_rset->isNull(colIdx)) {
      return nullopt;
    } else {
      return m_rset->getDouble(colIdx);
    }
  } catch(exception::Exception &ne) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      ne.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " + m_stmt.getSql() + ": " +
      se.what());
  }
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
