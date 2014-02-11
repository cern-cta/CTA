/******************************************************************************
 *                      clientSimulator.cpp
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

#include <typeinfo>
#include <memory>

#include "clientSimulator.hpp"
#include "../../tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"

using namespace castor::tape::server;
using namespace castor::tape;

clientSimulator::clientSimulator(uint32_t volReqId, std::string vid):
  TpcpCommand("clientSimulator::clientSimulator"), m_vid(vid)
{
  m_volReqId = volReqId;
  setupCallbackSock();
}


void clientSimulator::processFirstRequest()
throw (castor::exception::Exception) {
  // Accept the next connection
  std::auto_ptr<castor::io::ServerSocket> clientConnection(m_callbackSock.accept());
  // Read in the message sent by the tapebridge
  std::auto_ptr<castor::IObject> obj(clientConnection->readObject());
  // Cast the message to a GatewayMessage or die
  tapegateway::GatewayMessage & msg(
      dynamic_cast<tapegateway::GatewayMessage &> (*obj.get()));
  // Check the mount transaction ID
  if (msg.mountTransactionId() != (uint64_t) m_volReqId) {
    std::stringstream oss;
    oss <<
        "Mount transaction ID mismatch" <<
        ": actual=" << msg.mountTransactionId() <<
        " expected=" << m_volReqId;
    sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
        oss.str(), *clientConnection);
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << oss.str();
    throw ex;
  }
  // We strictly expect getVolId for this test for the moment
  if (msg.type() != tapegateway::VolumeRequest::TYPE()) {
    std::stringstream oss;
    oss <<
        "Expected a tapegateway::VolumeRequest, got " << typeid (msg).name();
    sendEndNotificationErrorReport(msg.aggregatorTransactionId(), EBADMSG,
        oss.str(), *clientConnection);
    castor::exception::Exception ex(EBADMSG);
    ex.getMessage() << oss.str();
    throw ex;
  }
  // This should always succeed
  tapegateway::VolumeRequest & vReq =
      dynamic_cast<tapegateway::VolumeRequest &> (msg);
  tapegateway::Volume vol;
  vol.setAggregatorTransactionId(vReq.aggregatorTransactionId());
  vol.setVid(m_vid);
  vol.setClientType(tapegateway::READ_TP);
  vol.setMode(castor::tape::tapegateway::READ);
  vol.setLabel(m_volLabel);
  vol.setMountTransactionId(m_volReqId);
  vol.setDensity(m_density);
  clientConnection->sendObject(vol);
}

void clientSimulator::sendEndNotificationErrorReport(
    const uint64_t tapebridgeTransactionId,
    const int errorCode,
    const std::string &errorMessage,
    castor::io::AbstractSocket &sock)
throw () {
  tapegateway::EndNotificationErrorReport err;
  err.setAggregatorTransactionId(tapebridgeTransactionId);
  err.setErrorCode(errorCode);
  err.setErrorMessage(errorMessage);
  sock.sendObject(err);
}