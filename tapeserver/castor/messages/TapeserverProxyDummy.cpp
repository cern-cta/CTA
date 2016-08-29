/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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
  const std::list<castor::log::Param> & params) {
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
