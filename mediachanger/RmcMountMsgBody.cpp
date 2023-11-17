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

#include "mediachanger/RmcMountMsgBody.hpp"

#include <string.h>

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RmcMountMsgBody::RmcMountMsgBody():
  uid(0),
  gid(0),
  side(0),
  drvOrd(0) {
  memset(unusedLoader, '\0', sizeof(unusedLoader));
  memset(vid, '\0', sizeof(vid));
}

uint32_t RmcMountMsgBody::bodyLen() const {
  const auto vidLen = strnlen(vid, CA_MAXVIDLEN+1);
  if(*unusedLoader != '\0' || vidLen > CA_MAXVIDLEN) {
    throw exception::Exception("Message body contains improperly-terminated strings");
  }

  auto retval = sizeof(uid) +
                sizeof(gid) +
                sizeof(unusedLoader) +
                vidLen + 1 +
                sizeof(side) +
                sizeof(drvOrd);
  return static_cast<uint32_t>(retval);
}

} // namespace cta::mediachanger
