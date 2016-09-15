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

#include "DriveHandlerProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"

namespace cta { namespace tape { namespace daemon {

DriveHandlerProxy::DriveHandlerProxy(server::SocketPair& socketPair): m_socketPair(socketPair) {
  m_socketPair.close(server::SocketPair::Side::parent);  
}

void DriveHandlerProxy::addLogParams(const std::string& unitName, const std::list<cta::log::Param>&   params) {
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
  // TODO
}

void DriveHandlerProxy::deleteLogParams(const std::string& unitName, const std::list<std::string>& paramNames) {
  // TODO
  throw 0;
}

void DriveHandlerProxy::labelError(const std::string& unitName, const std::string& message) {
  // TODO
  throw 0;
}

void DriveHandlerProxy::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {
  // TODO
  throw 0;
}

void DriveHandlerProxy::reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) {
  // TODO
  throw 0;
}


DriveHandlerProxy::~DriveHandlerProxy() { }



}}} // namespace cta::tape::daemon
