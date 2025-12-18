/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/RmcMountMsgBody.hpp"

#include <string.h>

namespace cta::mediachanger {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RmcMountMsgBody::RmcMountMsgBody() {
  memset(unusedLoader, '\0', sizeof(unusedLoader));
  memset(vid, '\0', sizeof(vid));
}

uint32_t RmcMountMsgBody::bodyLen() const {
  const auto vidLen = strnlen(vid, CA_MAXVIDLEN + 1);
  if (*unusedLoader != '\0' || vidLen > CA_MAXVIDLEN) {
    throw exception::Exception("Message body contains improperly-terminated strings");
  }

  auto retval = sizeof(uid) + sizeof(gid) + sizeof(unusedLoader) + vidLen + 1 + sizeof(side) + sizeof(drvOrd);
  return static_cast<uint32_t>(retval);
}

}  // namespace cta::mediachanger
