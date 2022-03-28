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

#include <sstream>
#include <string>

#include "XrootCl.hpp"

namespace cta { namespace exception {

XrootCl::XrootCl(const XrdCl::XRootDStatus& status, const std::string & what) {
  if (!what.empty()) {
    getMessage() << what << " ";
  }
  getMessage() << status.ToStr().c_str();
  getMessage() << " code:"   << status.code
               << " errNo:"  << status.errNo
               << " status:" << status.status;
}

void XrootCl::throwOnError(const XrdCl::XRootDStatus& status, const std::string& context) {
  if (!status.IsOK()) {
    throw XrootCl(status, context);
  }
}

}  // namespace exception
}  // namespace cta
