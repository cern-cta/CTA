/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/utils.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace common {
namespace dataStructures {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DriveState::DriveState():
  sessionId(0),
  bytesTransferedInSession(0),
  filesTransferedInSession(0),
  latestBandwidth(0),
  sessionStartTime(0),
  currentStateStartTime(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool DriveState::operator==(const DriveState &rhs) const {
  return name==rhs.name
      && host==rhs.host
      && logicalLibrary==rhs.logicalLibrary
      && sessionId==rhs.sessionId
      && bytesTransferedInSession==rhs.bytesTransferedInSession
      && filesTransferedInSession==rhs.filesTransferedInSession
      && latestBandwidth==rhs.latestBandwidth
      && sessionStartTime==rhs.sessionStartTime
      && currentStateStartTime==rhs.currentStateStartTime
      && mountType==rhs.mountType
      && status==rhs.status
      && currentVid==rhs.currentVid
      && currentTapePool==rhs.currentTapePool;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool DriveState::operator!=(const DriveState &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const DriveState &obj) {
  os << "(name=" << obj.name
     << " host=" << obj.host
     << " logicalLibrary=" << obj.logicalLibrary
     << " sessionId=" << obj.sessionId
     << " bytesTransferedInSession=" << obj.bytesTransferedInSession
     << " filesTransferedInSession=" << obj.filesTransferedInSession
     << " latestBandwidth=" << obj.latestBandwidth
     << " sessionStartTime=" << obj.sessionStartTime
     << " currentStateStartTime=" << obj.currentStateStartTime
     << " mountType=" << obj.mountType
     << " status=" << obj.status
     << " currentVid=" << obj.currentVid
     << " currentTapePool=" << obj.currentTapePool << ")";
  return os;
}

} // namespace dataStructures
} // namespace common
} // namespace cta
