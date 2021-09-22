/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
 * This struct holds the information about who's issued the CTA command and from 
 * which host 
 */
struct SecurityIdentity {

  SecurityIdentity();
  
  SecurityIdentity(const std::string & username, const std::string & host);

  SecurityIdentity(const std::string & username, const std::string & host, const std::string & clientHost);

  bool operator==(const SecurityIdentity &rhs) const;

  bool operator!=(const SecurityIdentity &rhs) const;

  bool operator<(const SecurityIdentity &rhs) const;

  std::string username;
  std::string host;
  std::string clientHost;

}; // struct SecurityIdentity

std::ostream &operator<<(std::ostream &os, const SecurityIdentity &obj);

} // namespace dataStructures
} // namespace common
} // namespace cta
