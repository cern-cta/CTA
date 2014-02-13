/******************************************************************************
 *                      tapeSession.hpp
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <memory>

#include "tapeSession.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"

using namespace castor::tape;

castor::IObject *
  castor::tape::server::tapeSession::requestReplySession(
    tapegateway::GatewayMessage &req) {
  // Open connection to client
  castor::io::ClientSocket clientConnection(m_client.port, m_client.ip);
  clientConnection.connect();
  clientConnection.sendObject(req);
  castor::IObject * ret = clientConnection.readObject();
  return ret;
}

void castor::tape::server::tapeSession::getVolume() {
  tapegateway::VolumeRequest volReq;
  volReq.setMountTransactionId(m_mountTransactionId);
  std::auto_ptr<castor::IObject> reply(requestReplySession(volReq));
  // Get a Volume object or die
  tapegateway::Volume & vol = dynamic_cast<tapegateway::Volume &>(*reply);
  m_vid = vol.vid();
}
