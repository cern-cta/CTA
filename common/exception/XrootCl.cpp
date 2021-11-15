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
