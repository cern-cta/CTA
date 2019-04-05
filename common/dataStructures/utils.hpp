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

#pragma once

#include "common/dataStructures/TapeFile.hpp"

#include <iostream>
#include <map>

namespace cta {
namespace common {
namespace dataStructures {
  
std::ostream &operator<<(std::ostream &os, const std::list<TapeFile> &map);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::string> &map);
std::ostream &operator<<(std::ostream &os, const std::pair<std::string,std::string> &pair);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::pair<std::string,std::string>> &map);
std::ostream &operator<<(std::ostream &os, const std::map<std::string,std::pair<uint32_t,TapeFile>> &map);
std::ostream &operator<<(std::ostream &os, const std::map<uint64_t,std::pair<std::string,std::string>> &map);

} // namespace dataStructures
} // namespace common
} // namespace cta