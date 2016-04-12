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

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::DriveState::DriveState():
  bytesTransferedInSession(0),
  currentStateStartTime(0),
  filesTransferedInSession(0),
  latestBandwidth(0),
  sessionId(0),
  sessionStartTime(0) {}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool cta::common::dataStructures::DriveState::operator==(const DriveState &rhs) const {
  return bytesTransferedInSession==rhs.bytesTransferedInSession
      && currentStateStartTime==rhs.currentStateStartTime
      && currentTapePool==rhs.currentTapePool
      && currentVid==rhs.currentVid
      && filesTransferedInSession==rhs.filesTransferedInSession
      && host==rhs.host
      && latestBandwidth==rhs.latestBandwidth
      && logicalLibrary==rhs.logicalLibrary
      && mountType==rhs.mountType
      && name==rhs.name
      && sessionId==rhs.sessionId
      && sessionStartTime==rhs.sessionStartTime
      && status==rhs.status;
}

//------------------------------------------------------------------------------
// operator!=
//------------------------------------------------------------------------------
bool cta::common::dataStructures::DriveState::operator!=(const DriveState &rhs) const {
  return !operator==(rhs);
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const cta::common::dataStructures::DriveState &obj) {
  os << "(bytesTransferedInSession=" << obj.bytesTransferedInSession
     << " currentStateStartTime=" << obj.currentStateStartTime
     << " currentTapePool=" << obj.currentTapePool
     << " currentVid=" << obj.currentVid
     << " filesTransferedInSession=" << obj.filesTransferedInSession
     << " host=" << obj.host
     << " latestBandwidth=" << obj.latestBandwidth
     << " logicalLibrary=" << obj.logicalLibrary
     << " mountType=" << obj.mountType
     << " name=" << obj.name
     << " sessionId=" << obj.sessionId
     << " sessionStartTime=" << obj.sessionStartTime
     << " status=" << obj.status << ")";
  return os;
}

