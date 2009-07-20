/******************************************************************************
 *                 castor/tape/tpcp/ActionHandler.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/GatewayMessage.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tpcp/ActionHandler.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>


//------------------------------------------------------------------------------
// AbstractMsgHandler destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ActionHandler::AbstractMsgHandler::~AbstractMsgHandler() {
  // Do nothing
}


//------------------------------------------------------------------------------
// MsgHandlerMap destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ActionHandler::MsgHandlerMap::~MsgHandlerMap() {
  for(iterator itor=begin(); itor!=end(); itor++) {
    // Delete the MsgHandler
    delete(itor->second);
  }
}


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ActionHandler::ActionHandler(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
  const uint64_t volReqId, castor::io::ServerSocket &callbackSocket) throw() :
  m_debug(debug),
  m_tapeFseqRanges(tapeFseqRanges),
  m_filenames(filenames),
  m_filenameItor(filenames.begin()),
  m_vmgrTapeInfo(vmgrTapeInfo),
  m_dgn(dgn),
  m_volReqId(volReqId),
  m_callbackSocket(callbackSocket),
  m_fileTransactionId(1) {
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ActionHandler::~ActionHandler() {
  // Do nothing
}


//------------------------------------------------------------------------------
// dispatchMessage
//------------------------------------------------------------------------------
bool castor::tape::tpcp::ActionHandler::dispatchMessage()
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the aggregator
  int connectionSocketFd = 0;

  // Wait for a callback connection from the aggregator
  {
    bool waitForCallback    = true;
    while(waitForCallback) {
      try {
        connectionSocketFd = net::acceptConnection(m_callbackSocket.socket(),
          WAITCALLBACKTIMEOUT);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {
        std::cout << "Waited " << WAITCALLBACKTIMEOUT << " seconds for a "
        "callback connection from the tape server." << std::endl
        << "Continuing to wait." <<  std::endl;
      }
    }
  }

  // If debug, then display a textual description of the aggregator
  // callback connection
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "ActionHandler: Aggregator connection = ";
    net::writeSocketDescription(os, connectionSocketFd);
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket sock(connectionSocketFd);

  // Read in the message sent by the aggregator
  std::auto_ptr<castor::IObject> obj(sock.readObject());

  // If debug, then display the type of message received from the aggregator
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "ActionHandler: Received aggregator message of type = "
       << utils::objectTypeToString(obj->type()) << std::endl;
  }

  {
    // Cast the message to a GatewayMessage so that the mount transaction ID
    // can be checked
    tapegateway::GatewayMessage *msg =
      dynamic_cast<tapegateway::GatewayMessage *>(obj.get());
    if(msg == NULL) {
      std::stringstream oss;

      oss <<
        "Unexpected object type" <<
        ": Actual=" << utils::objectTypeToString(obj->type()) <<
        " Expected=Subclass of GatewayMessage";

      sendEndNotificationErrorReport(SEINTERNAL, oss.str(), sock);

      TAPE_THROW_EX(castor::exception::Internal, oss.str());
    }

    // Check the mount transaction ID
    if(msg->mountTransactionId() != m_volReqId) {
      std::stringstream oss;

      oss <<
        "Mount transaction ID mismatch" <<
        ": Actual=" << msg->mountTransactionId() <<
        " Expected=" << m_volReqId;

      sendEndNotificationErrorReport(EBADMSG, oss.str(), sock);

      castor::exception::Exception ex(EBADMSG);
      ex.getMessage() << oss.str();
      throw ex;
    }
  }

  // Find the message type's corresponding handler
  MsgHandlerMap::const_iterator itor = m_msgHandlers.find(obj->type());
  if(itor == m_msgHandlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
         ": Received unexpected aggregator message: "
      << ": Message type = " << utils::objectTypeToString(obj->type()));
  }
  const AbstractMsgHandler &handler = *itor->second;

  // Invoke the handler
  const bool moreWork = handler(obj.get(), sock);

  // Close the aggregator callback connection
  sock.close();

  return moreWork;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::ActionHandler::handleEndNotification(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::EndNotification *const endNotification =
    dynamic_cast<tapegateway::EndNotification*>(msg);
  if(endNotification == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=EndNotification");
  }

  // If debug, then display endNotification
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "ActionHandler: Received EndNotification from aggregator = ";
    StreamHelper::write(os, *endNotification);
    os << std::endl;
  }

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);

  // Send the NotificationAcknowledge message to the aggregator
  sock.sendObject(acknowledge);

  // If debug, then display sending of the NotificationAcknowledge message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Sent NotificationAcknowledge to aggregator = ";
    StreamHelper::write(os, acknowledge);
    os << std::endl;
  }

  return false;
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::ActionHandler::handleEndNotificationErrorReport(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::EndNotificationErrorReport *const
    endNotificationErrorReport =
    dynamic_cast<tapegateway::EndNotificationErrorReport*>(msg);
  if(endNotificationErrorReport == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=EndNotificationErrorReport");
  }

  // If debug, then display EndNotificationErrorReport
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "ActionHandler: "
          "Received EndNotificationErrorReport from aggregator = ";
    StreamHelper::write(os, *endNotificationErrorReport);
    os << std::endl;
  }

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);

  // Send the NotificationAcknowledge message to the aggregator
  sock.sendObject(acknowledge);

  // If debug, then display sending of the NotificationAcknowledge message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "ActionHandler: Sent NotificationAcknowledge to aggregator = ";
    StreamHelper::write(os, acknowledge);
    os << std::endl;
  }

  return false;
}


//------------------------------------------------------------------------------
// tellAggregatorNoMoreFiles
//------------------------------------------------------------------------------
void castor::tape::tpcp::ActionHandler::acknowledgeEndOfSession()
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the aggregator
  int connectionSocketFd = 0;

  // Wait for a callback connection from the aggregator
  {
    bool waitForCallback = true;
    while(waitForCallback) {
      try {
        connectionSocketFd = net::acceptConnection(m_callbackSocket.socket(),
          WAITCALLBACKTIMEOUT);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {
        std::cout << "Waited " << WAITCALLBACKTIMEOUT << " seconds for a "
        "callback connection from the tape server." << std::endl
        << "Continuing to wait." <<  std::endl;
      }
    }
  }

  // If debug, then display a textual description of the aggregator
  // callback connection
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "ActionHandler: Aggregator connection");
    os << std::endl;
    net::writeSocketDescription(os, connectionSocketFd);
    os << std::endl;
    os << std::endl;
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket sock(connectionSocketFd);

  // Read in the object sent by the aggregator
  std::auto_ptr<castor::IObject> obj(sock.readObject());

  // Pointer to the received object with the object's type
  tapegateway::EndNotification *endNotification = NULL;

  // Cast the object to its type
  endNotification = dynamic_cast<tapegateway::EndNotification*>(obj.get());
  if(endNotification == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
     << "Received the wrong type of object from the aggregator"
     << ": Actual=" << utils::objectTypeToString(obj->type())
     << " Expected=EndNotification";

    throw ex;
  }

  // If debug, then display reception of the EndNotification message
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "ActionHandler: Received EndNotification from "
      "aggregator");
    os << std::endl;
    os << std::endl;
  }

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);

  // Send the volume message to the aggregator
  sock.sendObject(acknowledge);

  // Close the connection to the aggregator
  sock.close();

  // If debug, then display sending of the NotificationAcknowledge message
  if(m_debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "Sent NotificationAcknowledge to aggregator");
    os << std::endl;
    os << std::endl;
  }
}


//------------------------------------------------------------------------------
// sendEndNotificationErrorReport
//------------------------------------------------------------------------------
void castor::tape::tpcp::ActionHandler::sendEndNotificationErrorReport(
  const int errorCode, const std::string &errorMessage,
  castor::io::AbstractSocket &sock) throw() {

  try {
    // Create the message
    tapegateway::EndNotificationErrorReport errorReport;
    errorReport.setErrorCode(errorCode);
    errorReport.setErrorMessage(errorMessage);

    // Send the message to the aggregator
    sock.sendObject(errorReport);

    // If debug, then display sending of message
    if(m_debug) {
      std::ostream &os = std::cout;

      os << "ActionHandler: Sent EndNotificationErrorReport to aggregator = ";
      StreamHelper::write(os, errorReport);
      os << std::endl;
    }
  } catch(...) {
    // Do nothing
  }
}
