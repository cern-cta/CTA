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
  throw cta::exception::Exception("In DriveHandlerProxy::addLogParams(): not implemented");
  // TODO
}

void DriveHandlerProxy::deleteLogParams(const std::string& unitName, const std::list<std::string>& paramNames) {
  // TODO
  throw cta::exception::Exception("In DriveHandlerProxy::deleteLogParams(): not implemented");
}

void DriveHandlerProxy::labelError(const std::string& unitName, const std::string& message) {
  // TODO
  throw cta::exception::Exception("In DriveHandlerProxy::labelError(): not implemented");
}

void DriveHandlerProxy::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {
  // TODO
  throw cta::exception::Exception("In DriveHandlerProxy::reportHeartbeat(): not implemented");
}

void DriveHandlerProxy::reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_totaldiskbytesmoved(0);
  watchdogMessage.set_totaltapebytesmoved(0);
  watchdogMessage.set_sessionstate((uint32_t)state);
  watchdogMessage.set_sessiontype((uint32_t)type);
 // TODO: add VID.
  std::string buffer;
  watchdogMessage.SerializeToString(&buffer);
  m_socketPair.send(buffer);
}


DriveHandlerProxy::~DriveHandlerProxy() { }



}}} // namespace cta::tape::daemon
