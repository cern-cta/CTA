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

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity() {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity(const std::string& username, const std::string& host):
  username(username), host(host) {}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity(const std::string& username, const std::string& host, const std::string & clientHost):
  username(username), host(host), clientHost(clientHost) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool SecurityIdentity::operator==(const SecurityIdentity &rhs) const {
  return username==rhs.username
      && host==rhs.host
      && clientHost==rhs.clientHost;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool SecurityIdentity::operator!=(const SecurityIdentity &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool SecurityIdentity::operator<(const SecurityIdentity &rhs) const {
  if(username == rhs.username) {
    if (host == rhs.host) {
      return clientHost < rhs.clientHost;
    } else {
      return host < rhs.host;
    }
  } else {
    return username < rhs.username;
  }
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const SecurityIdentity &obj) {
  os << "(username=" << obj.username
     << " host=" << obj.host 
     << " clientHost=" << obj.clientHost << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
