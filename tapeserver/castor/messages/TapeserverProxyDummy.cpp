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

#include "castor/messages/TapeserverProxyDummy.hpp"

//------------------------------------------------------------------------------
// reportState
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::reportState(const cta::tape::session::SessionState state, 
  const cta::tape::session::SessionType type, const std::string& vid) {}

//------------------------------------------------------------------------------
// reportHeartbeat
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) {}

//------------------------------------------------------------------------------
// addLogParams
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::
addLogParams(const std::string &unitName,
  const std::list<cta::log::Param> & params) {
}

//------------------------------------------------------------------------------
// deleteLogParans
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::
deleteLogParams(const std::string &unitName,
  const std::list<std::string> & paramNames) {
}

//------------------------------------------------------------------------------
// socketPair
//------------------------------------------------------------------------------
const std::unique_ptr<cta::server::SocketPair> &castor::messages::TapeserverProxyDummy::socketPair() {
  throw cta::exception::Exception(std::string("In ")+__PRETTY_FUNCTION__+": not implemented");
}
