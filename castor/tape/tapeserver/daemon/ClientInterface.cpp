/******************************************************************************
 *                      clientInterface.cpp
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

#include "ClientInterface.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/utils/Timer.hpp"
#include <cxxabi.h>
#include <memory>
#include <stdlib.h>
#include <typeinfo>

castor::tape::server::
ClientInterface::ClientInterface(const legacymsg::RtcpJobRqstMsgBody& clientRequest)
  throw (castor::tape::Exception): m_request(clientRequest) {}

castor::tape::server::ClientInterface::UnexpectedResponse::
    UnexpectedResponse(const castor::IObject* resp, const std::string & w):
castor::tape::Exception(w) {
  std::string responseType = typeid(*resp).name();
  int status = -1;
  char * demangled = abi::__cxa_demangle(responseType.c_str(), NULL, NULL, &status);
  if (!status)
    responseType = demangled;
  free(demangled);
  getMessage() << " Response type was: " << responseType;
}

tapegateway::GatewayMessage *
  castor::tape::server::ClientInterface::requestResponseSession(
    const tapegateway::GatewayMessage &req,
    RequestReport & report)
throw (castor::tape::Exception) 
{
  // 0) Start the stopwatch
  castor::tape::utils::Timer timer;
  // 1) We re-open connection to client for each request-response exchange
  castor::io::ClientSocket clientConnection(m_request.clientPort, 
      m_request.clientHost);
  clientConnection.connect();
  report.connectDuration = timer.secs();
  // 2) The actual exchange over the network.
  clientConnection.sendObject(const_cast<tapegateway::GatewayMessage &>(req));
  std::auto_ptr<castor::IObject> resp (clientConnection.readObject());
  report.sendRecvDuration = timer.secs();
  // 3) Check response type
  tapegateway::GatewayMessage * ret = 
      dynamic_cast<tapegateway::GatewayMessage*>(resp.get());
  if (NULL == ret) {
    throw UnexpectedResponse(resp.get(), 
        "In castor::tape::server::clientInterface::requestResponseSession, "
        "expected a tapegateway::GatewayMessage response. ");
  }
  // 4) Check we get a response for the request we sent (sanity check)
  if ((ret->mountTransactionId() != m_request.volReqId) ||
      ret->aggregatorTransactionId() != req.aggregatorTransactionId()) {
    
  }
  // Slightly ugly sequence in order to not duplicate the dynamic_cast.
  resp.release();
  return ret;
}

void castor::tape::server::ClientInterface::fetchVolumeId(
  VolumeInfo & volInfo, RequestReport &report) 
throw (castor::tape::Exception) {
  // 1) Build the request
  castor::tape::tapegateway::VolumeRequest request;
  request.setMountTransactionId(m_request.volReqId);
  // The transaction id is auto-incremented (through the cast operator) 
  // so we just need to use it (and save the value locally if we need to reuse 
  // it)
  report.transactionId = m_transactionId;
  request.setAggregatorTransactionId(report.transactionId);
  request.setUnit(m_request.driveUnit);
  // 2) get the reply from the client
  std::auto_ptr<tapegateway::GatewayMessage> 
      response (requestResponseSession(request, report));
  // 3) Process the possible outputs
  tapegateway::Volume * volume;
  tapegateway::NoMoreFiles * noMore;
  tapegateway::EndNotificationErrorReport * errorReport;
  // 3) a) Volume: this is the good day answer
  if (NULL != (volume = dynamic_cast<tapegateway::Volume *>(response.get()))) {
    volInfo.vid = volume->vid();
    volInfo.clientType = volume->clientType();
    volInfo.density = volume->density();
    volInfo.labelObsolete = volume->label();
    volInfo.volumeMode = volume->mode();
  // 3) b) Straight noMoreFiles answer: at least we know.
  } else if (NULL != (noMore = dynamic_cast<tapegateway::NoMoreFiles *>(response.get()))) {
    throw EndOfSession("Client replied noMoreFiles directly to volume request");
  // 3) c) End notification error report.
  } else if (NULL != (errorReport = dynamic_cast<tapegateway::EndNotificationErrorReport *>(
      response.get()))) {
    EndOfSession eos("Client replied noMoreFiles directly to volume request: ");
        eos.getMessage() << "errorCode=" << errorReport->errorCode()
        << "errorReport=\"" <<  errorReport->errorMessage() << "\"";
  // Unexpected response type  
  } else {
    throw UnexpectedResponse(response.get(), "Unexpected response from client in response "
        "to a volume request");
  }
}

void castor::tape::server::ClientInterface::reportEndOfSession(
RequestReport &transactionReport) 
throw (castor::tape::Exception) {
  // 1) Build the report
  castor::tape::tapegateway::EndNotification endReport;
  endReport.setMountTransactionId(m_request.volReqId);
  endReport.setAggregatorTransactionId(m_transactionId);
  // 2) Send the report
  std::auto_ptr<tapegateway::GatewayMessage> ack(
      requestResponseSession(endReport, transactionReport));
  // 3) If we did not get a ack, complain (not much more we can do)
  // We could use the castor typing here, but we stick to case for homogeneity
  // of the code.
  try {
    dynamic_cast<tapegateway::NotificationAcknowledge &>(*ack.get());
  } catch (std::bad_cast) {
    throw UnexpectedResponse(ack.get(), 
        "Unexpected response when reporting end of session");
  }
}
