/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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
// labelError
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::labelError(
  const std::string &unitName, const std::string &message) {
}
