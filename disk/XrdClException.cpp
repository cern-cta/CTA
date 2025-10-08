/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <sstream>
#include <string>

#include "XrdClException.hpp"

namespace cta::exception {

XrdClException::XrdClException(const XrdCl::XRootDStatus& status, std::string_view what) {
  if(!what.empty()) {
    getMessage() << what << " ";
  }
  getMessage() << status.ToStr().c_str();
  getMessage() << " code:"   << status.code
               << " errNo:"  << status.errNo
               << " status:" << status.status;
}

void XrdClException::throwOnError(const XrdCl::XRootDStatus& status, std::string_view context) {
  if(!status.IsOK()) {
    throw XrdClException(status, context);
  }
}

} // namespace cta::exception
