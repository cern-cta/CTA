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

#include "common/checksum/Checksum.hpp"
#include "common/utils/Regex.hpp"
#include <sstream>

//------------------------------------------------------------------------------
// checksumTypeToStr
//------------------------------------------------------------------------------
const char *cta::Checksum::checksumTypeToStr(const ChecksumType enumValue)
  throw() {
  switch(enumValue) {
  case CHECKSUMTYPE_NONE   : return "NONE";
  case CHECKSUMTYPE_ADLER32: return "ADLER32";
  default                  : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::Checksum::Checksum(): m_type(CHECKSUMTYPE_NONE) { }


cta::Checksum::Checksum(const std::string& url): m_type(CHECKSUMTYPE_NONE) {
  if (url.empty() || url == "-") {
    return;
  }
  utils::Regex re("^adler32:0[Xx]([[:xdigit:]]+)$");
  auto result = re.exec(url);
  if (result.size()) {
    m_type = CHECKSUMTYPE_ADLER32;
    std::stringstream valStr(result.at(1));
    uint32_t val;
    valStr >> std::hex >> val;
    setNumeric(val);
  }
}


//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
void cta::Checksum::validate(const Checksum &rhs) const {
  if(m_type != rhs.m_type) {
    throw ChecksumTypeMismatch(static_cast<std::string>("Checksum type ") + checksumTypeToStr(m_type) + " does not match " + checksumTypeToStr(rhs.m_type));
  }
  if(m_byteArray != rhs.m_byteArray) {
    throw ChecksumValueMismatch(static_cast<std::string>("Checksum value ") + m_byteArray + " does not match " + rhs.m_byteArray);
  }
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::Checksum::operator==(const Checksum &rhs) const {
  return m_type == rhs.m_type && m_byteArray == rhs.m_byteArray;
}

//------------------------------------------------------------------------------
// getType
//------------------------------------------------------------------------------
cta::Checksum::ChecksumType cta::Checksum::getType() const throw() {
  return m_type;
}

//------------------------------------------------------------------------------
// getByteArray
//------------------------------------------------------------------------------
const std::string &cta::Checksum::getByteArray() const throw() {
  return m_byteArray;
}

//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
std::string cta::Checksum::str() const {
  std::ostringstream oss;

  switch(m_type) {
    case CHECKSUMTYPE_ADLER32:
      oss << "adler32:" << std::hex << std::showbase << getNumeric<uint32_t>();
      break;
    case CHECKSUMTYPE_NONE:
      oss << "-";
      break;
    default:;
  }
  return oss.str();
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &cta::operator<<(std::ostream &os, const Checksum &checksum) {
  os << checksum.str();
  return os;
}
