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

#include "mediachanger/RmcMountMsgBody.hpp"

#include <string.h>

namespace cta {
namespace mediachanger {

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

} // namespace mediachanger
} // namespace cta
