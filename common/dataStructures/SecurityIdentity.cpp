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

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity() :
  authProtocol(Protocol::NONE) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity(const std::string& username, const std::string& host) :
  username(username), host(host), authProtocol(Protocol::NONE) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SecurityIdentity::SecurityIdentity(const std::string& username, const std::string& host, const std::string& clientHost, const std::string& auth) :
  username(username), host(host), clientHost(clientHost), authProtocol(Protocol::NONE) {
  if(!auth.empty()) {
    // Map the client protocol string to enum value
    auto proto_it = m_authProtoMap.find(auth);
    authProtocol = proto_it != m_authProtoMap.end() ? proto_it->second : Protocol::OTHER;
  }
}

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
