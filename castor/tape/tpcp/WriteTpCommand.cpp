/******************************************************************************
 *                 castor/tape/tpcp/WriteTpCommand.cpp
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
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/tpcp/WriteTpCommand.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rfio_api.h"

#include <errno.h>
#include <getopt.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::WriteTpCommand::WriteTpCommand() throw() :
  m_nbMigratedFiles(0) {

  // Register the Aggregator message handler member functions
  registerMsgHandler(OBJ_FileToMigrateRequest,
    &WriteTpCommand::handleFileToMigrateRequest, this);
  registerMsgHandler(OBJ_FileMigratedNotification,
    &WriteTpCommand::handleFileMigratedNotification, this);
  registerMsgHandler(OBJ_EndNotification,
    &WriteTpCommand::handleEndNotification, this);
  registerMsgHandler(OBJ_EndNotificationErrorReport,
    &WriteTpCommand::handleEndNotificationErrorReport, this);
  registerMsgHandler(OBJ_PingNotification,
    &WriteTpCommand::handlePingNotification, this);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::WriteTpCommand::~WriteTpCommand() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::usage(std::ostream &os,
  const char *const programName) throw() {
  os <<
    "Usage:\n"
    "\t" << programName << " VID POSITION [OPTIONS] [FILE]...\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID      The VID of the tape to be written to.\n"
    "\tPOSITION Either \"EOD\" meaning append to End-Of-Tape, or a tape file\n"
    "\t         sequence number equal to or greater than 1.\n"
    "\tFILE     A filename in RFIO notation [host:]local_path.\n"
    "\n"
    "Options:\n"
    "\n"
    "\t-d, --debug         Turn on the printing of debug information.\n"
    "\t-h, --help          Print this help and exit.\n"
    "\t-s, --server server Specifies the tape server to be used, therefore\n"
    "\t                    overriding the drive scheduling of the VDQM.\n"
    "\t-f, --filelist file File containing a list of filenames in RFIO\n"
    "\t                    notation [host:]local_path\n"
    "\n"
    "Constraints:\n"
    "\n"
    "\tThe [FILE].. command-line arguments and the \"-f, --filelist\" option\n"
    "\tare mutually exclusive\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

  m_cmdLine.action = Action::write;

  static struct option longopts[] = {
    {"debug"   , 0, NULL, 'd'},
    {"filelist", 1, NULL, 'f'},
    {"help"    , 0, NULL, 'h'},
    {"server"  , 1, NULL, 's'},
    {NULL      , 0, NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":df:hs:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      m_cmdLine.debugSet = true;
      break;

    case 'f':
      m_cmdLine.fileListSet = true;
      m_cmdLine.fileListFilename  = optarg;
      break;

    case 'h':
      m_cmdLine.helpSet = true;
      break;

    case 's':
      m_cmdLine.serverSet = true;
      try {
        utils::copyString(m_cmdLine.server, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the server command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
      break;

    case ':':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "\tThe -" << (char)optopt
          << " option requires a parameter";
        throw ex;
      }
      break;

    case '?':
      {
        castor::exception::InvalidArgument ex;

        if(optopt == 0) {
          ex.getMessage() << "\tUnknown command-line option";
        } else {
          ex.getMessage() << "\tUnknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
      break;

    default:
      {
        castor::exception::Internal ex;
        ex.getMessage()
          << "\tgetopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;
        throw ex;
      }
    } // switch (c)
  } // while ((c = getopt_long(argc, argv, "h", longopts, NULL)) != -1)

  // There is no need to continue parsing when the help option is set
  if( m_cmdLine.helpSet) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc-optind;

  // Check the VID has been specified
  if(nbArgs < 1) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tThe VID and POSITION have not been specified";

    throw ex;
  }

  // Check the POSITION has been specified
  if(nbArgs < 2) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "\tThe POSITION has not been specified";

    throw ex;
  }

  // -2 = -1 for the VID and -1 for the POSITION
  const int nbFilenamesOnCommandLine = nbArgs - 2;

  // Filenames on the command-line and the "-f, --filelist" option are mutually
  // exclusive
  if(nbFilenamesOnCommandLine > 0 && m_cmdLine.fileListSet) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\t[FILE].. command-line arguments and the"
       " \"-f, --filelist\" option are\n\tmutually exclusive";

    throw ex;
  }

  // Check the first command-line argument is syntactically a valid VID
  try {
    utils::checkVidSyntax(argv[optind]);
  } catch(castor::exception::InvalidArgument &ex) {
    castor::exception::InvalidArgument ex2;

    ex2.getMessage() << "\tFirst command-line argument must be a valid VID:\n"
      "\t" << ex.getMessage().str();

    throw ex2;
  }

  // Parse the VID command-line argument
  try {
    utils::copyString(m_cmdLine.vid, argv[optind]);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to copy VID comand-line argument into the internal data"
      " structures"
      ": " << ex.getMessage().str());
  }

  // Move on to the next command-line argument
  optind++;

  // Parse the POSITION command-line argument
  {
    std::string tmp(argv[optind]);
    utils::toUpper(tmp);

    if(tmp == "EOD") {
      m_cmdLine.tapeFseqPosition = 0;
    } else {

      if(!utils::isValidUInt(argv[optind])) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tSecond command-line argument must either be the string \"EOD\" "
          "or a valid\n\tunsigned integer greater than 0: Actual=\""
          << argv[optind] << "\"";
        throw ex;
      }

      m_cmdLine.tapeFseqPosition = atoi(tmp.c_str());

      if(m_cmdLine.tapeFseqPosition == 0) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tSecond command-line argument must be a valid unsigned integer "
          "greater\n\tthan 0: Actual=" << argv[optind];
        throw ex;
      }
    }
  }

  // Move on to the next command-line argument (there may not be one)
  optind++;

  // Parse any filenames on the command-line
  while(optind < argc) {
    m_cmdLine.filenames.push_back(argv[optind++]);
  }
}


//------------------------------------------------------------------------------
// performTransfer
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::performTransfer()
  throw (castor::exception::Exception) {

  m_nextTapeFseq = m_cmdLine.tapeFseqPosition;

  // Spin in the wait for and dispatch message loop until there is no more work
  while(waitForAndDispatchMessage()) {
    // Do nothing
  }

  const uint64_t nbMigrateRequests     = m_fileTransactionId - 1;
  const uint64_t nbIncompleteTransfers = m_pendingFileTransfers.size();

  std::ostream &os = std::cout;

  time_t now = time(NULL);
  utils::writeTime(os, now, TIMEFORMAT);
  os << " Finished writing to tape " << m_cmdLine.vid << std::endl
     << std::endl
     << "Number of files to be migrated   = " << m_filenames.size() << std::endl
     << "Number of migrate requests       = " << nbMigrateRequests << std::endl
     << "Number of successfull migrations = " << m_nbMigratedFiles << std::endl
     << "Number of incomplete transfers   = " << nbIncompleteTransfers
     << std::endl;

  if(m_pendingFileTransfers.size() > 0) {
    os << std::endl;
  }

  for(FileTransferMap::iterator itor=m_pendingFileTransfers.begin();
    itor!=m_pendingFileTransfers.end(); itor++) {

    uint64_t    fileTransactionId = itor->first;
    std::string &filename         = itor->second;

    os << "Incomplete transfer: fileTransactionId=" << fileTransactionId
       << " filename=" << filename
       << std::endl;
  }

  os << std::endl;
}


//------------------------------------------------------------------------------
// handleFileToMigrateRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleFileToMigrateRequest(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileToMigrateRequest *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  const bool anotherFile = m_filenameItor != m_filenames.end();

  if(anotherFile) {

    const std::string filename = *(m_filenameItor++);

    struct stat64 statBuf;

    // RFIO stat the disk file in order to check the RFIO daemon is running and
    // to get the file size
    try {
      rfioStat(filename.c_str(), statBuf);
    } catch(castor::exception::Exception &ex) {

      // Notify the aggregator about the exception and rethrow
      sendEndNotificationErrorReport(msg->aggregatorTransactionId(),
        ex.code(), ex.getMessage().str(), sock);
      throw(ex);
    }

    // Create FileToMigrate message for the aggregator
    tapegateway::FileToMigrate fileToMigrate;
    fileToMigrate.setMountTransactionId(m_volReqId);
    fileToMigrate.setAggregatorTransactionId(msg->aggregatorTransactionId());
    fileToMigrate.setFileTransactionId(m_fileTransactionId);
    fileToMigrate.setNshost("tpcp");
    fileToMigrate.setFileid(0);
    fileToMigrate.setFileSize(statBuf.st_size);
    fileToMigrate.setLastKnownFilename(filename);
    fileToMigrate.setLastModificationTime(statBuf.st_mtime);
    fileToMigrate.setUmask(RTCOPYCONSERVATIVEUMASK);
    fileToMigrate.setPath(filename);

    // If migrating to end-of-tape (EOD)
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
        " Migrating"
        " \"" << filename << "\"" << std::endl;
    }

  // Else no more files
  } else {

    // Create the NoMoreFiles message for the aggregator
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setMountTransactionId(m_volReqId);
    noMore.setAggregatorTransactionId(msg->aggregatorTransactionId());

    // Send the NoMoreFiles message to the aggregator
    sock.sendObject(noMore);

    Helper::displaySentMsgIfDebug(noMore, m_cmdLine.debugSet);
  }

  return true;
}


//------------------------------------------------------------------------------
// handleFileMigratedNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleFileMigratedNotification(
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

      sendEndNotificationErrorReport(msg->aggregatorTransactionId(), EBADMSG,
        oss.str(), sock);

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
       " Migrated"
       " \"" << filename << "\""
       " size=" << msg->fileSize() <<
       " checksum=0x" << std::hex << msg->checksum() << std::dec << std::endl;

    // The file has been transfer so remove it from the map of pending
    // transfers
    m_pendingFileTransfers.erase(itor);
  }

  // Update the count of successfull recalls
  m_nbMigratedFiles++;

  // Create the NotificationAcknowledge message for the aggregator
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);
  acknowledge.setAggregatorTransactionId(msg->aggregatorTransactionId());

  // Send the NotificationAcknowledge message to the aggregator
  sock.sendObject(acknowledge);

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);

  return true;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotificationErrorReport(obj, sock);
}


//------------------------------------------------------------------------------
// handlePingNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handlePingNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handlePingNotification(obj,sock);
}
