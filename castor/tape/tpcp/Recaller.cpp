/******************************************************************************
 *                 castor/tape/tpcp/Recaller.cpp
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
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Recaller.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Recaller::Recaller(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
  const int volReqId, castor::io::ServerSocket &callbackSocket) throw() :
  ActionHandler(debug, tapeFseqRanges, filenames, vmgrTapeInfo, dgn, volReqId,
  callbackSocket), m_tapeFseqSequence(tapeFseqRanges), m_nbRecalledFiles(0) {

  // Build the map of message body handlers
  m_handlers[OBJ_FileToRecallRequest] = &Recaller::handleFileToRecallRequest;
  m_handlers[OBJ_FileRecalledNotification] =
    &Recaller::handleFileRecalledNotification;
  m_handlers[OBJ_EndNotification] = &Recaller::handleEndNotification;
  m_handlers[OBJ_EndNotificationErrorReport] = 
    &Recaller::handleEndNotificationErrorReport;
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Recaller::~Recaller() {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::Recaller::run() throw(castor::exception::Exception) {

  // Spin in the dispatch message loop until there is no more work
  while(dispatchMessage()) {
    // Do nothing
  }

  const uint64_t nbRecallRequests      = m_fileTransactionId - 1;
  const uint64_t nbIncompleteTransfers = m_pendingFileTransfers.size();

  std::ostream &os = std::cout;

  utils::writeTime(os);
  os << ": End of recall session" << std::endl
     << std::endl
     << "Number of recall requests      = " << nbRecallRequests  << std::endl
     << "Number of successfull recalls  = " << m_nbRecalledFiles << std::endl
     << "Number of incomplete transfers = " << nbIncompleteTransfers
     << std::endl
     << std::endl;

  for(FileTransferMap::iterator itor=m_pendingFileTransfers.begin();
    itor!=m_pendingFileTransfers.end(); itor++) {

    uint64_t     fileTransactionId = itor->first;
    FileTransfer &fileTransfer     = itor->second;

    os << "Incomplete transfer: fileTransactionId=" << fileTransactionId
       << " tapeFseq=" << fileTransfer.tapeFseq
       << " filename=" << fileTransfer.filename
       << std::endl;
  }
}


//------------------------------------------------------------------------------
// dispatchMessage
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::dispatchMessage()
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

    os << "Recaller: Aggregator connection = ";
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

    os << "Recaller: Received aggregator message of type = "
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
// handleFileToRecallRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleFileToRecallRequest(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileToRecallRequest *const fileToRecallRequest =
    dynamic_cast<tapegateway::FileToRecallRequest*>(msg);
  if(fileToRecallRequest == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=FileToRecallRequest");
  }

  // If debug, then display the FileToRecallRequest message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Recaller: Received FileToRecallRequest from aggregator = ";
    StreamHelper::write(os, *fileToRecallRequest);
    os << std::endl;
  }

  // Check the mount transaction ID
  if(fileToRecallRequest->mountTransactionId() != m_volReqId) {
    castor::exception::Exception ex(EBADMSG);

    ex.getMessage()
      << "Mount transaction ID mismatch"
         ": Actual=" << fileToRecallRequest->mountTransactionId()
      << " Expected=" << m_volReqId;

    throw ex;
  }

  const bool anotherFile = m_tapeFseqSequence.hasMore() &&
    m_filenameItor != m_filenames.end();

  if(anotherFile) {
    // Get the tape file sequence number and RFIO filename
    const uint32_t    tapeFseq = m_tapeFseqSequence.next();
    const std::string filename = *(m_filenameItor++);

    // Create FileToRecall message for the aggregator
    tapegateway::FileToRecall fileToRecall;
    fileToRecall.setMountTransactionId(m_volReqId);
    fileToRecall.setFileTransactionId(m_fileTransactionId);
    fileToRecall.setNshost("tpcp\0");
    fileToRecall.setFileid(0);
    fileToRecall.setFseq(tapeFseq);
    fileToRecall.setPositionCommandCode(tapegateway::TPPOSIT_FSEQ);
    fileToRecall.setPath(filename);
    fileToRecall.setBlockId0(0);
    fileToRecall.setBlockId1(0);
    fileToRecall.setBlockId2(0);
    fileToRecall.setBlockId3(0);

    // Update the map of current file transfers and increment the file
    // transaction ID
    {
      FileTransfer fileTransfer = {tapeFseq, filename};
      m_pendingFileTransfers[m_fileTransactionId] = fileTransfer;
      m_fileTransactionId++;
    }

    // Send the FileToRecall message to the aggregator
    sock.sendObject(fileToRecall);

    // If debug, then display sending of the FileToRecall message
    if(m_debug) {
      std::ostream &os = std::cout;

      os << "Recaller: Sent FileToRecall to aggregator = ";
      StreamHelper::write(os, fileToRecall);
      os << std::endl;
    }

  // Else no more files
  } else {

    // Create the NoMoreFiles message for the aggregator
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setMountTransactionId(m_volReqId);

    // Send the NoMoreFiles message to the aggregator
    sock.sendObject(noMore);

    // If debug, then display sending of the NoMoreFiles message
    if(m_debug) {
      std::ostream &os = std::cout;

      utils::writeBanner(os, "Sent NoMoreFiles to aggregator");
      StreamHelper::write(os, noMore);
      os << std::endl;
    }
  }

  return true;
}


//------------------------------------------------------------------------------
// handleFileRecalledNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleFileRecalledNotification(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileRecalledNotification *const notification =
    dynamic_cast<tapegateway::FileRecalledNotification*>(msg);
  if(notification == NULL) {
    TAPE_THROW_EX(castor::exception::Internal,
         "Unexpected object type"
      << ": Actual=" << utils::objectTypeToString(msg->type())
      << " Expected=FileRecalledNotification");
  }

  // If debug, then display the FileRecalledNotification message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Recaller: Received FileRecalledNotification from aggregator = ";
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

  // Check the file transaction ID
  {
    FileTransferMap::iterator itor = 
      m_pendingFileTransfers.find(notification->fileTransactionId()); 

    // Throw an exception if the fileTransactionId is unknown
    if(itor == m_pendingFileTransfers.end()) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage()
        << "Received unknown file transaction ID from the aggregator"
           ": fileTransactionId="
        << notification->fileTransactionId();
      throw(ex);
    }

    // Command-line user feedback
    std::ostream &os = std::cout;
    FileTransfer &fileTransfer = itor->second;

    utils::writeTime(os);
    os << ": Recalled"
          ": tapeFseq="   << fileTransfer.tapeFseq
       <<  " filename=\"" << fileTransfer.filename << "\""
       << std::endl;

    // The file has been transfer so remove it from the map of pending
    // transfers
    m_pendingFileTransfers.erase(itor);
  }

  // Update the count of successfull recalls
  m_nbRecalledFiles++;

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);

  // Send the NotificationAcknowledge message to the aggregator
  sock.sendObject(acknowledge);

  // If debug, then display sending of the NotificationAcknowledge message
  if(m_debug) {
    std::ostream &os = std::cout;

    os << "Recaller: Sent NotificationAcknowledge to aggregator = ";
    StreamHelper::write(os, acknowledge);
    os << std::endl;
  }

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleEndNotification(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotification(msg, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleEndNotificationErrorReport(
  castor::IObject *msg, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotificationErrorReport(msg,sock);
}
