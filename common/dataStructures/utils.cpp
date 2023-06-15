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

#include "common/dataStructures/utils.hpp"

namespace cta {
namespace common {
namespace dataStructures {

std::ostream& operator<<(std::ostream& os, const std::list<TapeFile>& list) {
  os << "(";
  for (auto& af : list) {
    os << af << " ";
  }
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const std::map<uint64_t, std::string>& map) {
  os << "(";
  for (auto it = map.begin(); it != map.end(); it++) {
    os << " key=" << it->first << " value=" << it->second << " ";
  }
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const std::pair<std::string, std::string>& pair) {
  os << "(first=" << pair.first << " second=" << pair.second << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const std::map<uint64_t, std::pair<std::string, std::string>>& map) {
  os << "(";
  for (auto it = map.begin(); it != map.end(); it++) {
    os << " key=" << it->first << " value.first=" << it->second.first << "  value.second=" << it->second.second;
  }
  os << ")";
  return os;
}

std::ostream& operator<<(std::ostream& os, const std::map<std::string, std::pair<uint32_t, TapeFile>>& map) {
  os << "(";
  for (auto it = map.begin(); it != map.end(); it++) {
    os << " key=" << it->first << " value.first=" << it->second.first << "  value.second=" << it->second.second;
  }
  os << ")";
  return os;
}

}  // namespace dataStructures
}  // namespace common
}  // namespace cta
