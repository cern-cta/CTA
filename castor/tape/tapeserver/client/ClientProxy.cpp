/******************************************************************************
 *                      ClientProxy.cpp
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

#include "ClientProxy.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FilesToRecallListRequest.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/FileRecallReportList.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/utils/utils.hpp"
#include <cxxabi.h>
#include <memory>
#include <stdlib.h>
#include <typeinfo>

namespace castor {
namespace tape {
namespace tapeserver {
namespace client {
  

ClientProxy::ClientProxy(const legacymsg::RtcpJobRqstMsgBody& clientRequest):
m_request(clientRequest) {}

ClientProxy::UnexpectedResponse::
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
  ClientProxy::requestResponseSession(
    const tapegateway::GatewayMessage &req,
    RequestReport & report)
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
    std::stringstream mess;
    if (ret->mountTransactionId() != m_request.volReqId) {
    mess << "In castor::tape::server::clientInterface::requestResponseSession, "
        "expected a information about mountSessionId=" << m_request.volReqId
        << " and received: " << ret->mountTransactionId();
    } else {
    mess << "In castor::tape::server::clientInterface::requestResponseSession, "
        "expected a response for tapebridgeTransId=" << req.aggregatorTransactionId()
        << " and received for: " << ret->aggregatorTransactionId();
    }
    throw UnexpectedResponse(resp.get(), mess.str());
  }
  // Slightly ugly sequence in order to not duplicate the dynamic_cast.
  resp.release();
  return ret;
}

void ClientProxy::fetchVolumeId(
  VolumeInfo & volInfo, RequestReport &report) 
{
  // 1) Build the request
  castor::tape::tapegateway::VolumeRequest request;
  request.setMountTransactionId(m_request.volReqId);
  // The transaction id is auto-incremented (through the cast operator) 
  // so we just need to use it (and save the value locally if we need to reuse 
  // it)
  report.transactionId = ++m_transactionId;
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
        throw eos;
  // Unexpected response type  
  } else {
    throw UnexpectedResponse(response.get(), "Unexpected response from client in response "
        "to a volume request");
  }
}

void ClientProxy::reportEndOfSession(
RequestReport &transactionReport) 
{
  // 1) Build the report
  castor::tape::tapegateway::EndNotification endReport;
  transactionReport.transactionId = ++m_transactionId;
  endReport.setMountTransactionId(m_request.volReqId);
  endReport.setAggregatorTransactionId(transactionReport.transactionId);
  // 2) Send the report
  std::auto_ptr<tapegateway::GatewayMessage> ack(
      requestResponseSession(endReport, transactionReport));
  // 3) If we did not get a ack, complain (not much more we can do)
  // We could use the castor typing here, but we stick to case for homogeneity
  // of the code.
  try {
    // Here we are only interested by the fact that we received a 
    // notificationAcknowledge. The matching of the transactionId has already
    // been checked automatically by requestResponseSession.
    // The dynamic cast to reference will conveniently throw an exception
    // it we did not get the acknowledgement. We cast it to void to silence
    // some compilers (at least clang) which complain that the return value 
    // of the cast is not used.
    (void)dynamic_cast<tapegateway::NotificationAcknowledge &>(*ack.get());
  } catch (std::bad_cast) {
    throw UnexpectedResponse(ack.get(), 
        "Unexpected response when reporting end of session");
  }
}


void ClientProxy::reportEndOfSessionWithError(
const std::string & errorMsg, int errorCode, RequestReport &transactionReport) 
{
  // 1) Build the report
  castor::tape::tapegateway::EndNotificationErrorReport endReport;
  transactionReport.transactionId = ++m_transactionId;
  endReport.setMountTransactionId(m_request.volReqId);
  endReport.setAggregatorTransactionId(transactionReport.transactionId);
  endReport.setErrorMessage(errorMsg);
  endReport.setErrorCode(errorCode);
  // 2) Send the report
  std::auto_ptr<tapegateway::GatewayMessage> ack(
      requestResponseSession(endReport, transactionReport));
  // 3) If we did not get a ack, complain (not much more we can do)
  // We could use the castor typing here, but we stick to case for homogeneity
  // of the code.
  try {
    // Here we are only interested by the fact that we received a 
    // notificationAcknowledge. The matching of the transactionId has already
    // been checked automatically by requestResponseSession.
    // The dynamic cast to reference will conveniently throw an exception
    // it we did not get the acknowledgement. We cast it to void to silence
    // some compilers (at least clang) which complain that the return value 
    // of the cast is not used.
    (void)dynamic_cast<tapegateway::NotificationAcknowledge &>(*ack.get());
  } catch (std::bad_cast) {
    throw UnexpectedResponse(ack.get(), 
        "Unexpected response when reporting end of session");
  }
}


tapegateway::FilesToMigrateList * 
    ClientProxy::getFilesToMigrate(
uint64_t files, uint64_t bytes, RequestReport& report) 
{
  // 1) Build the request
  castor::tape::tapegateway::FilesToMigrateListRequest ftmReq;
  report.transactionId = ++m_transactionId;
  ftmReq.setMountTransactionId(m_request.volReqId);
  ftmReq.setAggregatorTransactionId(report.transactionId);
  ftmReq.setMaxBytes(files);
  ftmReq.setMaxBytes(bytes);
  // 2) Exchange messages with the server
  std::auto_ptr<tapegateway::GatewayMessage> resp(
      requestResponseSession(ftmReq, report));
  // 3) We expect either a NoMoreFiles response or FilesToRecallList
  // 3a) Handle the FilesToRecallListl
  try {
    tapegateway::FilesToMigrateList & ftm  =
        dynamic_cast <tapegateway::FilesToMigrateList &>(*resp);
    if (ftm.filesToMigrate().size()) {
      resp.release();
      return &ftm;
    } else {
      return NULL;
    }
  } catch (std::bad_cast) {}
  // 3b) Try again with NoMoreFiles (and this time failure is fatal)
  try {
    // As in reportEndOfSession, we are only interested in receiving a 
    // NoMoreFiles message. (void) for picky compilers
    (void)dynamic_cast<tapegateway::NoMoreFiles &>(*resp);
    return NULL;
  } catch (std::bad_cast) {
    throw UnexpectedResponse(resp.get(),
        "Unexpected response to FilesToMigrateListRequest in getFilesToMigrate");
  }
}

void ClientProxy::reportMigrationResults(
tapegateway::FileMigrationReportList & migrationReport,
    RequestReport& report) {
  // 1) The request is provided already fleshed out by the user. We have to
  // add the administrative numbering
  migrationReport.setMountTransactionId(m_request.volReqId);
  report.transactionId = ++m_transactionId;
  migrationReport.setAggregatorTransactionId(report.transactionId);
  // The next 2 parameters are currently set to hardcoded defaults (as were in
  // the tape bridge). They were created in prevision of an evolution where
  // mode responsibility of central servers updates was to be pushed to the 
  // tape server.
  migrationReport.setFseqSet(false);
  migrationReport.setFseq(0);
  // 2) Exchange messages with the server
  std::auto_ptr<tapegateway::GatewayMessage> resp(
      requestResponseSession(migrationReport, report));
  // 3) We expect 2 types of return messages: NotificationAcknowledge and
  // EndNotificationErrorReport.
  // 3a) Handle the NotificationAcknowledge
  try {
    // As in reportEndOfSession, we are only interested in receiving a 
    // NotificationAcknowledge message. (void) for picky compilers
    (void)dynamic_cast<tapegateway::NotificationAcknowledge &>(*resp);
    return;
  } catch (std::bad_cast) {}
  // 3b) Handle the end notification error report and turn it into a bare
  // tape::exception
  try {
    tapegateway::EndNotificationErrorReport & err = 
        dynamic_cast<tapegateway::EndNotificationErrorReport &> (*resp);
    std::stringstream mess("End notification report: errorMessage=\"");
    mess << err.errorMessage() << "\" errorCode=" << err.errorCode();
    throw castor::tape::Exception(mess.str());
  } catch (std::bad_cast) {
    throw UnexpectedResponse(resp.get(),
        "Unexpected response to FileMigrationReportList in reportMigrationResults");
  }
}

tapegateway::FilesToRecallList * 
    ClientProxy::getFilesToRecall(
uint64_t files, uint64_t bytes, RequestReport& report) 
{
  // 1) Build the request
  castor::tape::tapegateway::FilesToRecallListRequest ftrReq;
  report.transactionId = ++m_transactionId;
  ftrReq.setMountTransactionId(m_request.volReqId);
  ftrReq.setAggregatorTransactionId(report.transactionId);
  ftrReq.setMaxBytes(files);
  ftrReq.setMaxBytes(bytes);
  // 2) Exchange messages with the server
  std::auto_ptr<tapegateway::GatewayMessage> resp(
      requestResponseSession(ftrReq, report));
  // 3) We expect either a NoMoreFiles response or FilesToRecallList
  // 3a) Handle the FilesToRecallListl
  try {
    tapegateway::FilesToRecallList & ftr  =
        dynamic_cast <tapegateway::FilesToRecallList &>(*resp);
    if (ftr.filesToRecall().size()) {
      resp.release();
      return &ftr;
    } else {
      return NULL;
    }
  } catch (std::bad_cast) {}
  // 3b) Try again with NoMoreFiles (and this time failure is fatal)
  try {
    // As in reportEndOfSession, we are only interested in receiving a 
    // NoMoreFiles message. (void) for picky compilers
    (void)dynamic_cast<tapegateway::NoMoreFiles &>(*resp);
    return NULL;
  } catch (std::bad_cast) {
    throw UnexpectedResponse(resp.get(),
        "Unexpected response to FilesToRecallListRequest in getFilesToRecall");
  }
}

void ClientProxy::reportRecallResults(
tapegateway::FileRecallReportList & recallReport,
RequestReport& report) {
  // 1) The request is provided already fleshed out by the user. We have to
  // add the administrative numbering
  recallReport.setMountTransactionId(m_request.volReqId);
  report.transactionId = ++m_transactionId;
  recallReport.setAggregatorTransactionId(report.transactionId);
  // 2) Exchange messages with the server
  std::auto_ptr<tapegateway::GatewayMessage> resp(
      requestResponseSession(recallReport, report));
  // 3) We expect 2 types of return messages: NotificationAcknowledge and
  // EndNotificationErrorReport.
  // 3a) Handle the NotificationAcknowledge
  try {
    // As in reportEndOfSession, we are only interested in receiving a 
    // NotificationAcknowledge message. (void) for picky compilers
    (void)dynamic_cast<tapegateway::NotificationAcknowledge &>(*resp);
    return;
  } catch (std::bad_cast) {}
  // 3b) Handle the end notification error report and turn it into a bare
  // tape::exception
  try {
    tapegateway::EndNotificationErrorReport & err = 
        dynamic_cast<tapegateway::EndNotificationErrorReport &> (*resp);
    std::stringstream mess("End notification report: errorMessage=\"");
    mess << err.errorMessage() << "\" errorCode=" << err.errorCode();
    throw castor::tape::Exception(mess.str());
  } catch (std::bad_cast) {
    throw UnexpectedResponse(resp.get(),
        "Unexpected response to FileRecallReportList in reportRecallResults");
  }
}

}}}}
