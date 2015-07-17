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

#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePath.hpp"
#include "remotens/RemoteNSDispatcher.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RemoteNSDispatcher::~RemoteNSDispatcher() throw() {
  for(auto itor = m_handlers.begin(); itor != m_handlers.end(); itor++) {
    delete(itor->second);
  }
}

//------------------------------------------------------------------------------
// registerProtcolHandler
//------------------------------------------------------------------------------
void cta::RemoteNSDispatcher::registerProtocolHandler(
  const std::string &protocol, std::unique_ptr<RemoteNS> handler) {
  if(NULL == handler.get()) {
    std::ostringstream msg;
    msg << "Registering a NULL pointer as the handler for protocol " << protocol
      << " is no permitted";
    throw exception::Exception(msg.str());
  }

  auto itor = m_handlers.find(protocol);
  if(m_handlers.end() != itor) {
    std::ostringstream msg;
    msg << "Cannot register handler for protocol " << protocol <<
      " because there is already a handler for this protocol";
    throw exception::Exception(msg.str());
  }

  m_handlers[protocol] = handler.release();
}

//------------------------------------------------------------------------------
// statFile
//------------------------------------------------------------------------------
std::unique_ptr<cta::RemoteFileStatus> cta::RemoteNSDispatcher::statFile(
  const RemotePath &path) const {
  return getHandler(path.getScheme()).statFile(path);
}

//------------------------------------------------------------------------------
// getHandler
//------------------------------------------------------------------------------
const cta::RemoteNS &cta::RemoteNSDispatcher::getHandler(
  const std::string &protocol) const {
  auto itor = m_handlers.find(protocol);
  if(m_handlers.end() == itor) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Cannot find a handler for protocol " << protocol;
    throw exception::Exception(msg.str());
  }

  if(NULL == itor->second) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Found a null pointer when searching for the"
      " handler of the protocol " << protocol;
    throw exception::Exception(msg.str());
  }

  return *(itor->second);
}

//------------------------------------------------------------------------------
// getHandler
//------------------------------------------------------------------------------
cta::RemoteNS &cta::RemoteNSDispatcher::getHandler(
  const std::string &protocol) {
  auto itor = m_handlers.find(protocol);
  if(m_handlers.end() == itor) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Cannot find a handler for protocol " << protocol;
    throw exception::Exception(msg.str());
  }

  if(NULL == itor->second) {
    std::ostringstream msg;
    msg << __FUNCTION__ << " - Found a null pointer when searching for the"
      " handler of the protocol " << protocol;
    throw exception::Exception(msg.str());
  }

  return *(itor->second);
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
void cta::RemoteNSDispatcher::rename(const RemotePath &remoteFile,
  const RemotePath &newRemoteFile) {
  getHandler(remoteFile.getScheme()).rename(remoteFile, newRemoteFile);
}

//------------------------------------------------------------------------------
// getFilename
//------------------------------------------------------------------------------
std::string cta::RemoteNSDispatcher::getFilename(const RemotePath &remoteFile)
  const {
  return getHandler(remoteFile.getScheme()).getFilename(remoteFile);
}
