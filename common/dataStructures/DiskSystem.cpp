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

#include "DiskSystem.hpp"

#include <algorithm>

namespace cta {
namespace common {
namespace dataStructures {

const DiskSystem& DiskSystemList::at(const std::string& name) {
  auto dsi = std::find_if(begin(), end(), [&](const DiskSystem& ds){ return ds.name == name;});
  if (dsi != end()) return *dsi;
  throw std::out_of_range("In DiskSystemList::at(): name not found.");
}

std::string DiskSystemList::getFSNAme(const std::string& fileURL) {
  // First if the regexes have not been created yet, do so.
  if (m_pointersAndRegexes.empty() && size()) {
    for (const auto &ds: *this) {
      m_pointersAndRegexes.emplace_back(PointerAndRegex({ds, utils::Regex(ds.fileRegexp.c_str())}));
    }
  }
  // Try and find the fileURL
  auto pri = std::find_if(m_pointersAndRegexes.begin(), m_pointersAndRegexes.end(), 
      [&](const PointerAndRegex & pr){ return !pr.regex.exec(fileURL).empty(); });
  if (pri != m_pointersAndRegexes.end()) {
    // We found a match. Let's move the pointer and regex to the front so next file will be faster (most likely).
    if (pri != m_pointersAndRegexes.begin())
      m_pointersAndRegexes.splice(m_pointersAndRegexes.begin(), m_pointersAndRegexes, pri);
    return pri->ds.name;
  }
  throw std::out_of_range("In DiskSystemList::getFSNAme(): not match for fileURL");
  
}

} // namespace dataStructures
} // namespace common
} // namespace cta