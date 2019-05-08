/*!
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#include "ChecksumBlob.hpp"
#include "ChecksumBlobSerDeser.hpp"

namespace cta {
namespace checksum {

void ChecksumBlob::insert(ChecksumType type, const std::string &value) {
  // Validate the length of the checksum
  size_t expectedLength;
  switch(type) {
    case NONE:       expectedLength = 0;  break;
    case ADLER32:
    case CRC32:
    case CRC32C:     expectedLength = 4;  break;
    case MD5:        expectedLength = 16; break;
    case SHA1:       expectedLength = 20; break;
  }
  if(value.length() != expectedLength) throw exception::ChecksumValueMismatch(
    "Checksum type="    + ChecksumTypeName.at(type) +
    " length expected=" + std::to_string(expectedLength) +
    " actual="          + std::to_string(value.length()));
  m_cs[type] = value;
}

void ChecksumBlob::insert(ChecksumType type, uint32_t value) {
  // This method is only valid for 32-bit checksums
  std::string cs;
  switch(type) {
    case ADLER32:
    case CRC32:
    case CRC32C:
      for(int i = 0; i < 4; ++i) {
        cs.push_back(static_cast<char>(value & 0x0F));
        value >>= 1;
      }
      m_cs[type] = cs;
      break;
    default:
      throw exception::ChecksumTypeMismatch(ChecksumTypeName.at(type) + " is not a 32-bit checksum");
  }
}

void ChecksumBlob::validate(const ChecksumBlob &blob) const {
  if(m_cs.size() != blob.m_cs.size()) {
    throw exception::ChecksumBlobSizeMismatch("Checksum blob size does not match. expected=" +
      std::to_string(m_cs.size()) + " actual=" + std::to_string(blob.m_cs.size()));
  }

  auto it1 = m_cs.begin();
  auto it2 = blob.m_cs.begin();
  for( ; it1 != m_cs.end(); ++it1, ++it2) {
    throw exception::Exception("not implemented");
  }
}

std::string ChecksumBlob::serialize() const {
  common::ChecksumBlob p_csb;
  ChecksumBlobToProtobuf(*this, p_csb);

  std::string bytearray;
  p_csb.SerializeToString(&bytearray);
  return bytearray;
}

size_t ChecksumBlob::length() const {
  // Inefficient as it requires re-doing the serialization. Check if we really need this method.
  return serialize().length();
}

void ChecksumBlob::deserialize(const std::string &bytearray) {
//ProtobufToChecksumBlob(const common::ChecksumBlob &p_csb, checksum::ChecksumBlob &csb)
}

std::ostream &operator<<(std::ostream &os, const ChecksumBlob &csb) {
  throw exception::Exception("not implemented");
#if 0
  os << "(";
  for(auto &cs : csb.m_cs) {
    os << " " << cs << " ";
  }
  os << ")";
#endif
  return os;
}

}} // namespace cta::checksum
