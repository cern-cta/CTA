/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeLabelRqstMsgBody::TapeLabelRqstMsgBody() throw():
  lbp(0),
  force(0),
  uid(0),
  gid(0) {
  memset(vid, '\0', sizeof(vid));
  memset(drive, '\0', sizeof(drive));
  memset(logicalLibrary, '\0', sizeof(logicalLibrary));
}
