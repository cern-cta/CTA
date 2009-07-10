/******************************************************************************
 *                 castor/tape/tpcp/Migrator.cpp
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
 
#include "castor/Constants.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/EndNotificationErrorReport.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Migrator.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Migrator::Migrator(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
  const int volReqId, castor::io::ServerSocket &callbackSocket) throw() :
  ActionHandler(debug, tapeFseqRanges, filenames, vmgrTapeInfo, dgn, volReqId,
    callbackSocket) {

  // Build the map of message body handlers
  m_handlers[OBJ_FileToMigrateRequest] = &Migrator::handleFileToMigrateRequest;
  m_handlers[OBJ_FileMigratedNotification] =
    &Migrator::handleFileMigratedNotification;
  m_handlers[OBJ_EndNotification] = &Migrator::handleEndNotification;
  m_handlers[OBJ_EndNotificationErrorReport] =
    &Migrator::handleEndNotificationErrorReport;
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Migrator::~Migrator() {
  // Do nothing
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::Migrator::run() throw(castor::exception::Exception) {

  // Spin in the dispatch message loop until there is no more work
  while(dispatchMessage()) {
    // Do nothing
  }
}


//------------------------------------------------------------------------------
// dispatchMessage
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::dispatchMessage()
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
        std::cout << "Waited " << WAITCALLBACKTIMEOUT << "seconds for a "
        "callback connection from the tape server." << std::endl
        << "Continuing to wait." <<  std::endl;
      }
    }
  }

  // If debug, then display a textual description of the aggregator
  // callback connection
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Migrator: Aggregator connection = ";
    net::writeSocketDescription(os, connectionSocketFd);
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket callbackConnectionSocket(connectionSocketFd);

  // Read in the message sent by the aggregator
  std::auto_ptr<castor::IObject> msg(callbackConnectionSocket.readObject());

  // If debug, then display the type of message received from the aggregator
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Migrator: Received aggregator message of type = "
       << utils::objectTypeToString(msg->type()) << std::endl;
  }

  // Find the message type's corresponding handler
  MsgHandlerMap::const_iterator itor = m_handlers.find(msg->type());
  if(itor == m_handlers.end()) {
    TAPE_THROW_CODE(EBADMSG,
         ": Received unexpected aggregator message: "
      << ": Message type = " << utils::objectTypeToString(msg->type()));
  }
  const MsgHandler handler = itor->second;

  // Invoke the handler
  const bool moreWork = (this->*handler)(msg.get(), callbackConnectionSocket);

  // Close the aggregator callback connection
  callbackConnectionSocket.close();

  return moreWork;
}


//------------------------------------------------------------------------------
// handleFileToMigrateRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleFileToMigrateRequest(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

 tapegateway::FileToMigrateRequest *const fileToMigrateRequest =
    dynamic_cast<tapegateway::FileToMigrateRequest*>(msg);
  if(fileToMigrateRequest == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=FileToRecallRequest");
  }

  // If debug, then display the FileToRecallRequest message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Migrator: Received FileToMigrateRequest from aggregator = ";
    StreamHelper::write(os, *fileToMigrateRequest);
    os << std::endl;
  }

  // Check the mount transaction ID
  if(fileToMigrateRequest->mountTransactionId() != m_volReqId) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage()
      << "Mount transaction ID mismatch"
         ": Actual=" << fileToMigrateRequest->mountTransactionId()
      << " Expected=" << m_volReqId;

    throw ex;
  }


  return true;
}


//------------------------------------------------------------------------------
// handleFileMigratedNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleFileMigratedNotification(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileMigratedNotification *const notification =
    dynamic_cast<tapegateway::FileMigratedNotification*>(msg);
  if(notification == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=FileMigratedNotification");
  }

  // If debug, then display the FileMigratedNotification message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Migrator: Received FileMigratedNotification from aggregator = ";
    StreamHelper::write(os, *notification);
    os << std::endl;
  }

  // Check the mount transaction ID
  if(notification->mountTransactionId() != m_volReqId) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage()
      << "Mount transaction ID mismatch"
         ": Actual=" << notification->mountTransactionId()
      << " Expected=" << m_volReqId;

    throw ex;
  }

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleEndNotification(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotification(msg, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleEndNotificationErrorReport(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotificationErrorReport(msg, sock);
}
