/**
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

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>


namespace cta {
namespace common {
namespace dataStructures {

/**
 * This struct holds the username and group name of a given user 
 */
struct RequesterIdentity {

  RequesterIdentity();
  
  RequesterIdentity(const std::string &name, const std::string &group);

  bool operator==(const RequesterIdentity &rhs) const;

  bool operator!=(const RequesterIdentity &rhs) const;

  std::string name;
  std::string group;

}; // struct RequesterIdentity

std::ostream &operator<<(std::ostream &os, const RequesterIdentity &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
