/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ChecksumBlob.hpp"

#include <iomanip>

namespace cta::checksum {

void ChecksumBlob::insert(ChecksumType type, const std::string& value) {
  // Validate the length of the checksum
  size_t expectedLength = 0;
  // clang-format off
  switch(type) {
    case NONE:       expectedLength = 0;  break;
    case ADLER32:
    case CRC32:
    case CRC32C:     expectedLength = 4;  break;
    case MD5:        expectedLength = 16; break;
    case SHA1:       expectedLength = 20; break;
    case CRC64:
    case XXHASH64:
    case HWH64:      expectedLength = 8;  break;
    case SHA256:
    case BLAKE3:     expectedLength = 32; break;
  }
  // clang-format on
  if (value.length() > expectedLength) {
    throw exception::ChecksumValueMismatch("Checksum length type=" + ChecksumTypeName.at(type)
                                           + " expected=" + std::to_string(expectedLength)
                                           + " actual=" + std::to_string(value.length()));
  }
  // Pad bytearray to expected length with trailing zeros
  m_cs[type] = value + std::string(expectedLength - value.length(), 0);
}

void ChecksumBlob::insert(ChecksumType type, uint32_t value) {
  // This method is only valid for 32-bit checksums
  std::string cs;
  switch (type) {
    case ADLER32:
    case CRC32:
    case CRC32C:
      for (int i = 0; i < 4; ++i) {
        cs.push_back(static_cast<unsigned char>(value & 0xFF));
        value >>= 8;
      }
      m_cs[type] = cs;
      break;
    default:
      throw exception::ChecksumTypeMismatch(ChecksumTypeName.at(type) + " is not a 32-bit checksum");
  }
}

void ChecksumBlob::validateOn(ChecksumType type, const ChecksumBlob& blob) const {
  const auto otherChecksum = blob.m_cs.find(type);
  if (otherChecksum == blob.m_cs.end()) {
    throw exception::ChecksumTypeMismatch(ChecksumTypeName.at(type) + " checksum not found in other blob");
  }

  const auto thisChecksum = m_cs.find(type);
  if (thisChecksum == m_cs.end()) {
    throw exception::ChecksumTypeMismatch(ChecksumTypeName.at(type) + " checksum not found in this blob");
  }

  if (thisChecksum->second != otherChecksum->second) {
    throw exception::ChecksumValueMismatch("Checksum value expected=0x" + ByteArrayToHex(thisChecksum->second)
                                           + " actual=0x" + ByteArrayToHex(otherChecksum->second));
  }
}

void ChecksumBlob::validate(const ChecksumBlob& blob) const {
  bool foundCommonType = false;

  for (const auto& [type, value] : m_cs) {
    // both blobs may contain NONE
    // but that doesn't mean the file contents match
    if (type == NONE) {
      continue;
    }

    const auto otherChecksum = blob.m_cs.find(type);
    if (otherChecksum == blob.m_cs.end()) {
      continue;
    }

    foundCommonType = true;
    if (value != otherChecksum->second) {
      throw exception::ChecksumValueMismatch("Checksum value expected=0x" + ByteArrayToHex(value) + " actual=0x"
                                             + ByteArrayToHex(otherChecksum->second));
    }
  }

  if (!foundCommonType) {
    throw exception::ChecksumTypeMismatch("No common checksum type found between archive and tape files");
  }
}

std::string ChecksumBlob::HexToByteArray(std::string hexString) {
  std::string bytearray;

  if (hexString.substr(0, 2) == "0x" || hexString.substr(0, 2) == "0X") {
    hexString.erase(0, 2);
  }
  // ensure we have an even number of hex digits
  if (hexString.length() % 2 == 1) {
    hexString.insert(0, "0");
  }

  for (unsigned int i = 0; i < hexString.length(); i += 2) {
    uint8_t byte = strtol(hexString.substr(i, 2).c_str(), nullptr, 16);
    bytearray.insert(0, 1, byte);
  }

  return bytearray;
}

std::string ChecksumBlob::ByteArrayToHex(const std::string& bytearray) {
  if (bytearray.empty()) {
    return "0";
  }

  std::stringstream value;
  value << std::hex << std::setfill('0');
  for (auto c = bytearray.rbegin(); c != bytearray.rend(); ++c) {
    value << std::setw(2) << (static_cast<uint8_t>(*c) & 0xFF);
  }
  return value.str();
}

void ChecksumBlob::addFirstChecksumToLog(cta::log::ScopedParamContainer& spc) const {
  const auto& csItor = m_cs.begin();
  if (csItor != m_cs.end()) {
    const auto& [type, value] = *csItor;
    std::string checksumTypeParam = "checksumType";
    std::string checksumValueParam = "checksumValue";
    spc.add(checksumTypeParam, ChecksumTypeName.at(type)).add(checksumValueParam, ByteArrayToHex(value));
  }
}

std::ostream& operator<<(std::ostream& os, const ChecksumBlob& csb) {
  os << "[ ";
  auto num_els = csb.m_cs.size();
  for (const auto& [type, value] : csb.m_cs) {
    bool is_last_el = --num_els > 0;
    os << "{ \"" << ChecksumTypeName.at(type) << "\",0x" << ChecksumBlob::ByteArrayToHex(value)
       << (is_last_el ? " }," : " }");
  }
  os << " ]";

  return os;
}

}  // namespace cta::checksum
