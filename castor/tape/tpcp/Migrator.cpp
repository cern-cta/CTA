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
#include "castor/tape/tpcp/Helper.hpp"
#include "castor/tape/tpcp/Migrator.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rfio_api.h"

#include <errno.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Migrator::Migrator(ParsedCommandLine &cmdLine,
  FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
  const char *const dgn, const int volReqId,
  castor::io::ServerSocket &callbackSock) throw() :
  ActionHandler(cmdLine, filenames, vmgrTapeInfo, dgn, volReqId, callbackSock),
  m_nextTapeFseq(cmdLine.tapeFseqPosition), m_nbMigratedFiles(0) {

  // Register the Aggregator message handler member functions
  registerMsgHandler(OBJ_FileToMigrateRequest,
    &Migrator::handleFileToMigrateRequest, this);
  registerMsgHandler(OBJ_FileMigratedNotification,
    &Migrator::handleFileMigratedNotification, this);
  registerMsgHandler(OBJ_EndNotification,
    &Migrator::handleEndNotification, this);
  registerMsgHandler(OBJ_EndNotificationErrorReport,
    &Migrator::handleEndNotificationErrorReport, this);
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
  while(ActionHandler::dispatchMessage()) {
    // Do nothing
  }

  const uint64_t nbMigrateRequests     = m_fileTransactionId - 1;
  const uint64_t nbIncompleteTransfers = m_pendingFileTransfers.size();

  std::ostream &os = std::cout;

  time_t now = time(NULL);
  utils::writeTime(os, now, TIMEFORMAT);
  os << ": End of migrate session" << std::endl
     << std::endl
     << "Number of migrate requests       = " << nbMigrateRequests << std::endl
     << "Number of successfull migrations = " << m_nbMigratedFiles << std::endl
     << "Number of incomplete transfers   = " << nbIncompleteTransfers
     << std::endl
     << std::endl;

  for(FileTransferMap::iterator itor=m_pendingFileTransfers.begin();
    itor!=m_pendingFileTransfers.end(); itor++) {

    uint64_t    fileTransactionId = itor->first;
    std::string &filename         = itor->second;

    os << "Incomplete transfer: fileTransactionId=" << fileTransactionId
       << " filename=" << filename
       << std::endl;
  }
}


//------------------------------------------------------------------------------
// handleFileToMigrateRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleFileToMigrateRequest(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileToMigrateRequest *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  const bool anotherFile = m_filenameItor != m_filenames.end();

  if(anotherFile) {
    const std::string filename = *(m_filenameItor++);
    struct stat64     statBuf;
    const int         rc = rfio_stat64((char*)filename.c_str(), &statBuf);
    const int         save_serrno = serrno;

    if(rc != 0) {
      char buf[STRERRORBUFLEN];
      sstrerror_r(save_serrno, buf, sizeof(buf));
      buf[sizeof(buf)-1] = '\0';

      std::stringstream oss;

      oss <<
        "Failed to rfio_stat64 file \"" << filename << "\""
        ": " << buf;

      sendEndNotificationErrorReport(save_serrno, oss.str(), sock);

      castor::exception::Exception ex(save_serrno);

      ex.getMessage() << oss.str();
      throw(ex);
    }

    // Create FileToMigrate message for the aggregator
    tapegateway::FileToMigrate fileToMigrate;
    fileToMigrate.setMountTransactionId(m_volReqId);
    fileToMigrate.setFileTransactionId(m_fileTransactionId);
    fileToMigrate.setNshost("tpcp");
    fileToMigrate.setFileid(0);
    fileToMigrate.setFileSize(statBuf.st_size);
    fileToMigrate.setLastKnownFilename(filename);
    fileToMigrate.setLastModificationTime(statBuf.st_mtime);
    fileToMigrate.setPath(filename);

    // If migrating to end-of-tape (EOT)
    if(m_cmdLine.tapeFseqPosition == 0) {
      fileToMigrate.setPositionCommandCode(tapegateway::TPPOSIT_EOI);
      fileToMigrate.setFseq(-1);
    // Else migrating to a specific tape file sequence number
    } else {
      fileToMigrate.setPositionCommandCode(tapegateway::TPPOSIT_FSEQ);
      fileToMigrate.setFseq(m_nextTapeFseq++);
    }

    // Update the map of current file transfers and increment the file
    // transaction ID
    {
      m_pendingFileTransfers[m_fileTransactionId] = filename;
      m_fileTransactionId++;
    }

    // Send the FileToMigrate message to the aggregator
    sock.sendObject(fileToMigrate);

    Helper::displaySentMsgIfDebug(fileToMigrate, m_cmdLine.debugSet);

    {
      // Command-line user feedback
      std::ostream &os = std::cout;

      time_t now = time(NULL);
      utils::writeTime(os, now, TIMEFORMAT);
      os <<
        ": Migrating"
        " filename=\"" << filename << "\"" << std::endl;
    }

  // Else no more files
  } else {

    // Create the NoMoreFiles message for the aggregator
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setMountTransactionId(m_volReqId);

    // Send the NoMoreFiles message to the aggregator
    sock.sendObject(noMore);

    Helper::displaySentMsgIfDebug(noMore, m_cmdLine.debugSet);
  }

  return true;
}


//------------------------------------------------------------------------------
// handleFileMigratedNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleFileMigratedNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileMigratedNotification *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

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
    std::ostream &os       = std::cout;
    std::string  &filename = itor->second;

    time_t now = time(NULL);
    utils::writeTime(os, now, TIMEFORMAT);
    os <<
       ": Migrated"
       " filename=\"" << filename << "\""
       " size=" << msg->fileSize() <<
       " checskum=0x" << std::hex << msg->checksum() << std::dec << std::endl;

    // The file has been transfer so remove it from the map of pending
    // transfers
    m_pendingFileTransfers.erase(itor);
  }

  // Update the count of successfull recalls
  m_nbMigratedFiles++;

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);

  // Send the NotificationAcknowledge message to the aggregator
  sock.sendObject(acknowledge);

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::Migrator::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return ActionHandler::handleEndNotificationErrorReport(obj, sock);
}
