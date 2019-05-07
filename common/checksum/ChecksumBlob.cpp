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
#include "cta_common.pb.h"

namespace cta {

void ChecksumBlob::insert(Checksum::ChecksumType type, const std::string &value) {
  throw exception::Exception("not implemented");
}

void ChecksumBlob::insert(Checksum::ChecksumType type, uint32_t value) {
  throw exception::Exception("not implemented");
}

std::string ChecksumBlob::serialize() const {
  common::ChecksumBlob csb;

  for(auto &cs : m_cs) {
    auto p_cs = csb.add_cs();
    switch(cs.getType()) {
      case Checksum::CHECKSUMTYPE_NONE:
      default:
        throw exception::Exception("not implemented");
        p_cs->set_type(common::ChecksumBlob::Checksum::NONE);
    }
    p_cs->set_value(cs.getByteArray());
  }

  std::string s;
  csb.SerializeToString(&s);
  return s;
}

size_t ChecksumBlob::length() const {
  // Inefficient as it requires re-doing the serialization. Check if we really need this method.
  return serialize().length();
}

void ChecksumBlob::deserialize(const std::string &bytearray) {
  throw exception::Exception("not implemented");
}

std::ostream &operator<<(std::ostream &os, const cta::ChecksumBlob &csb) {
  os << "(";
  for(auto &cs : csb.m_cs) {
    os << " " << cs << " ";
  }
  os << ")";
  return os;
}

} // namespace cta
