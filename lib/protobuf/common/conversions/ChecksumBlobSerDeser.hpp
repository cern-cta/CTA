/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/checksum/ChecksumBlob.hpp"

#include "cta_common.pb.h"

namespace cta::checksum {

inline void ProtobufToChecksumBlob(const common::ChecksumBlob& p_csb, checksum::ChecksumBlob& csb) {
  csb.clear();
  for (auto& cs : p_csb.cs()) {
    checksum::ChecksumType type;
    switch (cs.type()) {
      case common::ChecksumBlob::Checksum::ADLER32:
        type = ADLER32;
        break;
      case common::ChecksumBlob::Checksum::CRC32:
        type = CRC32;
        break;
      case common::ChecksumBlob::Checksum::CRC32C:
        type = CRC32C;
        break;
      case common::ChecksumBlob::Checksum::MD5:
        type = MD5;
        break;
      case common::ChecksumBlob::Checksum::SHA1:
        type = SHA1;
        break;
      case common::ChecksumBlob::Checksum::NONE:
      default:
        type = NONE;
        break;
    }
    csb.insert(type, cs.value());
  }
}

inline void ChecksumBlobToProtobuf(const checksum::ChecksumBlob& csb, common::ChecksumBlob& p_csb) {
  for (const auto& [type, value] : csb.getMap()) {
    common::ChecksumBlob::Checksum::Type protobufType;
    switch (type) {
      case ADLER32:
        protobufType = common::ChecksumBlob::Checksum::ADLER32;
        break;
      case CRC32:
        protobufType = common::ChecksumBlob::Checksum::CRC32;
        break;
      case CRC32C:
        protobufType = common::ChecksumBlob::Checksum::CRC32C;
        break;
      case MD5:
        protobufType = common::ChecksumBlob::Checksum::MD5;
        break;
      case SHA1:
        protobufType = common::ChecksumBlob::Checksum::SHA1;
        break;
      case NONE:
      default:
        protobufType = common::ChecksumBlob::Checksum::NONE;
        break;
    }
    auto cs_ptr = p_csb.add_cs();
    cs_ptr->set_type(protobufType);
    cs_ptr->set_value(value);
  }
}

}  // namespace cta::checksum
