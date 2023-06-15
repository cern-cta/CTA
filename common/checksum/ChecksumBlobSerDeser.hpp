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

#pragma once

#include "common/checksum/ChecksumBlob.hpp"
#include "cta_common.pb.h"

namespace cta {
namespace checksum {

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
  for (auto& cs : csb.getMap()) {
    common::ChecksumBlob::Checksum::Type type;
    switch (cs.first) {
      case ADLER32:
        type = common::ChecksumBlob::Checksum::ADLER32;
        break;
      case CRC32:
        type = common::ChecksumBlob::Checksum::CRC32;
        break;
      case CRC32C:
        type = common::ChecksumBlob::Checksum::CRC32C;
        break;
      case MD5:
        type = common::ChecksumBlob::Checksum::MD5;
        break;
      case SHA1:
        type = common::ChecksumBlob::Checksum::SHA1;
        break;
      case NONE:
      default:
        type = common::ChecksumBlob::Checksum::NONE;
        break;
    }
    auto cs_ptr = p_csb.add_cs();
    cs_ptr->set_type(type);
    cs_ptr->set_value(cs.second);
  }
}

}  // namespace checksum
}  // namespace cta
