/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "lib/protobuf/common/conversions/ChecksumBlobSerDeser.hpp"

#include <limits>

namespace cta::checksum {

std::string ChecksumBlob::serialize() const {
  common::ChecksumBlob p_csb;
  ChecksumBlobToProtobuf(*this, p_csb);

  std::string bytearray;
  p_csb.SerializeToString(&bytearray);
  return bytearray;
}

size_t ChecksumBlob::length() const {
  common::ChecksumBlob p_csb;
  ChecksumBlobToProtobuf(*this, p_csb);
  return p_csb.ByteSizeLong();
}

void ChecksumBlob::deserialize(const std::string& bytearray) {
  common::ChecksumBlob p_csb;
  if (!p_csb.ParseFromString(bytearray)) {
    throw exception::Exception("ChecksumBlob: deserialization failed");
  }
  ProtobufToChecksumBlob(p_csb, *this);
}

void ChecksumBlob::deserialize(const void* data, std::size_t size) {
  common::ChecksumBlob p_csb;
  if (size > std::numeric_limits<int>::max()) {
    throw std::overflow_error("Blob too large (>2GB) for Protobuf ParseFromArray");
  }
  // In case we do not want to construct temporary std::strings beforehand and we just want to parse
  // raw binary data (e.g., from Postgres BLOB) we can do so directly with
  // ParseFromArray designed for fast, zero-copy parsing
  if (!p_csb.ParseFromArray(data, static_cast<int>(size))) {
    throw exception::Exception("ChecksumBlob: deserialization failed");
  }
  ProtobufToChecksumBlob(p_csb, *this);
}

void ChecksumBlob::deserializeOrSetAdler32(const std::string& bytearray, uint32_t adler32) {
  common::ChecksumBlob p_csb;
  // A nullptr value in the CHECKSUM_BLOB column will return an empty bytearray. If the bytearray is empty
  // or otherwise invalid, default to using the contents of the CHECKSUM_ADLER32 column.
  if (!bytearray.empty() && p_csb.ParseFromString(bytearray)) {
    ProtobufToChecksumBlob(p_csb, *this);
  } else {
    insert(ADLER32, adler32);
  }
}

}  // namespace cta::checksum
