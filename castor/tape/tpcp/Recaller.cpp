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
#include "castor/tape/tpcp/Helper.hpp"
#include "castor/tape/tpcp/Recaller.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>
#include <sstream>
#include <time.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Recaller::Recaller(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
  const int volReqId, castor::io::ServerSocket &callbackSocket) throw() :
  ActionHandler(debug, tapeFseqRanges, filenames, vmgrTapeInfo, dgn, volReqId,
  callbackSocket), m_tapeFseqSequence(tapeFseqRanges), m_nbRecalledFiles(0) {

  // Register the Aggregator message handler member functions
  registerMsgHandler(OBJ_FileToRecallRequest,
    &Recaller::handleFileToRecallRequest, this);
  registerMsgHandler(OBJ_FileRecalledNotification,
    &Recaller::handleFileRecalledNotification, this);
  registerMsgHandler(OBJ_EndNotification,
    &Recaller::handleEndNotification, this);
  registerMsgHandler(OBJ_EndNotificationErrorReport,
    &Recaller::handleEndNotificationErrorReport, this);
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
  while(ActionHandler::dispatchMessage()) {
    // Do nothing
  }

  const uint64_t nbRecallRequests      = m_fileTransactionId - 1;
  const uint64_t nbIncompleteTransfers = m_pendingFileTransfers.size();

  std::ostream &os = std::cout;

  time_t now = time(NULL);
  utils::writeTime(os, now, TIMEFORMAT);
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
// handleFileToRecallRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleFileToRecallRequest(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileToRecallRequest *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayReceivedMessageIfDebug(*msg, m_debug);

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

    {
      // Command-line user feedback
      std::ostream &os = std::cout;

      time_t now = time(NULL);
      utils::writeTime(os, now, TIMEFORMAT);
      os <<
        ": Recalling"
        " filename=\"" << filename << "\"" <<
        " fseq=" << tapeFseq <<  std::endl;
    }

    Helper::displaySentMessageIfDebug(fileToRecall, m_debug);

  // Else no more files
  } else {

    // Create the NoMoreFiles message for the aggregator
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setMountTransactionId(m_volReqId);

    // Send the NoMoreFiles message to the aggregator
    sock.sendObject(noMore);

    Helper::displaySentMessageIfDebug(noMore, m_debug);
  }

  return true;
}


//------------------------------------------------------------------------------
// handleFileRecalledNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleFileRecalledNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileRecalledNotification *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayReceivedMessageIfDebug(*msg, m_debug);

  // Check the file transaction ID
  {
    FileTransferMap::iterator itor = 
      m_pendingFileTransfers.find(msg->fileTransactionId()); 

    // If the fileTransactionId is unknown
    if(itor == m_pendingFileTransfers.end()) {
      std::stringstream oss;

      oss <<
        "Received unknown file transaction ID from the aggregator"
        ": fileTransactionId=" << msg->fileTransactionId();

      sendEndNotificationErrorReport(EBADMSG, oss.str(), sock);

      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() << oss.str();
      throw(ex);
    }

    // Command-line user feedback
    {
      std::ostream &os           = std::cout;
      FileTransfer &fileTransfer = itor->second;

      time_t now = time(NULL);
      utils::writeTime(os, now, TIMEFORMAT);
      os << 
        ": Recalled"
        " filename=\"" << fileTransfer.filename << "\""
        " fseq=" << fileTransfer.tapeFseq <<
        " size=" << msg->fileSize() <<
        " checskum=0x" << std::hex << msg->checksum() << std::dec << std::endl;
    }

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

  Helper::displaySentMessageIfDebug(acknowledge, m_debug);

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Recaller::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotificationErrorReport(obj,sock);
}
