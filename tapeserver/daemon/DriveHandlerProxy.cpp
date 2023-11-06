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

#include "DriveHandlerProxy.hpp"
#include "tapeserver/daemon/WatchdogMessage.pb.h"

namespace cta { namespace tape { namespace daemon {

DriveHandlerProxy::DriveHandlerProxy(server::SocketPair& socketPair): m_socketPair(socketPair) {
  m_socketPair.close(server::SocketPair::Side::parent);  
}

// TODO: me might want to group the messages to reduce the rate.

void DriveHandlerProxy::addLogParams(const std::string& unitName, const std::list<cta::log::Param>&   params) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(false);
  for (auto & p: params) {
    auto * lp = watchdogMessage.mutable_addedlogparams()->Add();
    lp->set_name(p.getName());
    lp->set_value(p.getValue());
  }
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::addLogParams(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  m_socketPair.send(buffer);
}

void DriveHandlerProxy::deleteLogParams(const std::string& unitName, const std::list<std::string>& paramNames) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(false);
  for (auto &pn: paramNames) {
    auto * lpn = watchdogMessage.mutable_deletedlogparams()->Add();
    *lpn = pn;
  }
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::deleteLogParams(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  m_socketPair.send(buffer);
}

void DriveHandlerProxy::labelError(const std::string& unitName, const std::string& message) {
  // TODO
  throw cta::exception::Exception("In DriveHandlerProxy::labelError(): not implemented");
}

void DriveHandlerProxy::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(false);
  watchdogMessage.set_reportingbytes(true);
  watchdogMessage.set_totaltapebytesmoved(totalTapeBytesMoved);
  watchdogMessage.set_totaldiskbytesmoved(totalDiskBytesMoved);
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportHeartbeat(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  m_socketPair.send(buffer);
}

void DriveHandlerProxy::reportState(const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid) {
  serializers::WatchdogMessage watchdogMessage;
  watchdogMessage.set_reportingstate(true);
  watchdogMessage.set_reportingbytes(false);
  watchdogMessage.set_totaldiskbytesmoved(0);
  watchdogMessage.set_totaltapebytesmoved(0);
  watchdogMessage.set_sessionstate((uint32_t)state);
  watchdogMessage.set_sessiontype((uint32_t)type);
  watchdogMessage.set_vid(vid);
  std::string buffer;
  if (!watchdogMessage.SerializeToString(&buffer)) {
    throw cta::exception::Exception(std::string("In DriveHandlerProxy::reportState(): could not serialize: ")+
        watchdogMessage.InitializationErrorString());
  }
  m_socketPair.send(buffer);
}

}}} // namespace cta::tape::daemon
