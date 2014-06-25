/******************************************************************************
 *         castor/messages/TapeserverProxyDummy.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#include "castor/messages/TapeserverProxyDummy.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::TapeserverProxyDummy::TapeserverProxyDummy() throw() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::messages::TapeserverProxyDummy::~TapeserverProxyDummy() throw() {
}

//------------------------------------------------------------------------------
// gotReadMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::gotReadMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
}

//------------------------------------------------------------------------------
// gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------
uint64_t
  castor::messages::TapeserverProxyDummy::gotWriteMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {

  return 0;  // Always return 0 files on tape
}

//------------------------------------------------------------------------------
// gotDumpMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::gotDumpMountDetailsFromClient(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
}

//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::tapeMountedForRead(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
}

//------------------------------------------------------------------------------
// tapeMountedForWrite
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::tapeMountedForWrite(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
}

//------------------------------------------------------------------------------
// tapeUnmounting
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::tapeUnmounting(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
} 

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::messages::TapeserverProxyDummy::tapeUnmounted(
  castor::tape::tapeserver::client::ClientProxy::VolumeInfo volInfo,
  const std::string &unitName) {
} 

std::auto_ptr<castor::tape::tapeserver::daemon::TaskWatchDog> 
castor::messages::TapeserverProxyDummy::createWatchdog(log::LogContext& lc) const{
  return std::auto_ptr<castor::tape::tapeserver::daemon::TaskWatchDog>(
          new castor::tape::tapeserver::daemon::DummyTaskWatchDog(lc)
          );
}