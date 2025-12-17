/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/utils.hpp"

namespace cta::common::dataStructures {

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

}  // namespace cta::common::dataStructures
