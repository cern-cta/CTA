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
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToMigrateListRequest.hpp"
#include "castor/tape/tapegateway/FileMigrationReportList.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Helper.hpp"
#include "castor/tape/tpcp/WriteTpCommand.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "h/Cupv_api.h"
#include "h/rfio_api.h"

#include <errno.h>
#include <getopt.h>
#include <memory>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::WriteTpCommand::WriteTpCommand() throw() :
  TpcpCommand("writetp"),
  m_nbMigratedFiles(0) {

  // Register the Tapebridge message handler member functions
  registerMsgHandler(OBJ_FilesToMigrateListRequest, this,
    &WriteTpCommand::handleFilesToMigrateListRequest);
  registerMsgHandler(OBJ_FileMigrationReportList, this,
    &WriteTpCommand::handleFileMigrationReportList);
  registerMsgHandler(OBJ_EndNotification, this,
    &WriteTpCommand::handleEndNotification);
  registerMsgHandler(OBJ_EndNotificationErrorReport, this,
    &WriteTpCommand::handleEndNotificationErrorReport);
  registerMsgHandler(OBJ_PingNotification, this,
    &WriteTpCommand::handlePingNotification);
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
void castor::tape::tpcp::WriteTpCommand::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " VID [OPTIONS] [FILE]...\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID      The VID of the tape to be written to.\n"
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

    ex.getMessage() << "\tThe VID has not been specified";

    throw ex;
  }

  const int nbFilenamesOnCommandLine = nbArgs - 1;

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

  // Move on to the next command-line argument.  There may not actually be one because
  // the user may have specified the list of disk source files using the -f option
  // instead of listing them at the end of the command-line arguments.
  optind++;

  // Parse any filenames at the of the command-line
  while(optind < argc) {
    m_cmdLine.filenames.push_back(argv[optind++]);
  }
}


//------------------------------------------------------------------------------
// checkAccessToDisk
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::checkAccessToDisk()
  const throw (castor::exception::Exception) {
  // Check that there is at least one file to be migrated
  if(m_filenames.size() == 0) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "There must be at least one file to be migrated";

    throw ex;
  }

  // RFIO stat the first file to be migrated in order to check the RFIO
  // daemon is running
  {
    FilenameList::const_iterator itor = m_filenames.begin();

    // Sanity check - there should be at least one file to be migrated
    if(itor == m_filenames.end()) {
      TAPE_THROW_EX(exception::Internal,
        ": List of filenames to be processed is unexpectedly empty");
    }

    const std::string &filename = *itor;

    // Command-line user feedback
    std::ostream &os = std::cout;
    time_t       now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os << " RFIO stat the first file \""
       << filename << "\"" << std::endl;

    // Perform the RFIO stat
    struct stat64 statBuf;
    rfioStat(filename.c_str(), statBuf);

    // Test if the filename corrispond to a directory
    if( (statBuf.st_mode & S_IFMT) == S_IFDIR ){
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        ": Invalid RFIO filename syntax"
        ": Filename must identify a regular file"
        ": filename=\"" << filename.c_str() <<"\"";

      throw ex;
    }

    // Test if the filesize is greather than zero
    if(statBuf.st_size == 0){
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        ": Invalid file size: File size must be greater than zero"
        ": filename=\"" << filename.c_str() <<"\"";

      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// checkAccessToTape
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::checkAccessToTape()
  const throw(castor::exception::Exception) {
  checkUserHasTapeWritePermission(m_vmgrTapeInfo.poolname, m_userId,
    m_groupId, m_hostname);

  if(m_vmgrTapeInfo.status & DISABLED ||
    m_vmgrTapeInfo.status & EXPORTED ||
    m_vmgrTapeInfo.status & ARCHIVED ||
    m_vmgrTapeInfo.status & TAPE_RDONLY) {
    castor::exception::Exception ex(ECANCELED);
    std::ostream &os = ex.getMessage();
    os <<
      "Tape is not available for writing"
      ": Tape is";
    if(m_vmgrTapeInfo.status & DISABLED) os << " DISABLED";
    if(m_vmgrTapeInfo.status & EXPORTED) os << " EXPORTED";
    if(m_vmgrTapeInfo.status & ARCHIVED) os << " ARCHIVED";
    if(m_vmgrTapeInfo.status & TAPE_RDONLY) os << " TAPE_RDONLY";

    throw ex;
  }
}


//------------------------------------------------------------------------------
// checkUserHasTapeWritePermission
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::checkUserHasTapeWritePermission(
  const char *const poolName, const uid_t userId, const gid_t groupId,
  const char *const sourceHost)
  const throw(castor::exception::PermissionDenied) {

  // Get the owner of the pool by querying the VMGR
  uid_t poolUserId  = 0;
  gid_t poolGroupId = 0;
  {
    const int rc = vmgr_querypool(poolName, &poolUserId,
      &poolGroupId, NULL, NULL);
    const int saved_serrno = serrno;

    if(rc < 0) {
      castor::exception::Exception ex(saved_serrno);

      ex.getMessage() <<
        "Failed to query tape pool"
        ": poolName=" << poolName <<
        ": " << sstrerror(saved_serrno);
    }
  }

  const bool userOwnsPool = userId == poolUserId && groupId == poolGroupId;

  if(userOwnsPool) {
    // Command-line user feedback
    std::ostream &os = std::cout;
    time_t       now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os <<
      " User can write to tape: User owns tape pool \"" << poolName << "\"" <<
      std::endl;
  } else {

    const bool userIsAdmin =
      Cupv_check(userId, groupId, sourceHost, "TAPE_SERVERS",P_ADMIN) == 0 ||
      Cupv_check(userId, groupId, sourceHost, NULL          ,P_ADMIN) == 0;

    if(userIsAdmin) {
      // Command-line user feedback
      std::ostream &os = std::cout;
      time_t       now = time(NULL);

      utils::writeTime(os, now, TIMEFORMAT);
      os <<
        " User can write to tape: User has ADMIN privilege" << std::endl;
    } else {
      castor::exception::PermissionDenied ex;

      ex.getMessage() <<
        "User cannot write to tape"
        ": User must own the \"" << poolName << "\" tape pool or "
        "have the ADMIN privilege";

      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// requestDriveFromVdqm
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::requestDriveFromVdqm(
  char *const tapeServer) throw(castor::exception::Exception) {
  TpcpCommand::requestDriveFromVdqm(WRITE_ENABLE, tapeServer);
}


//------------------------------------------------------------------------------
// sendVolumeToTapeBridge
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::sendVolumeToTapeBridge(
  const tapegateway::VolumeRequest &volumeRequest,
  castor::io::AbstractTCPSocket    &connection)
  const throw(castor::exception::Exception) {
  castor::tape::tapegateway::Volume volumeMsg;
  volumeMsg.setVid(m_vmgrTapeInfo.vid);
  volumeMsg.setClientType(castor::tape::tapegateway::WRITE_TP);
  volumeMsg.setMode(castor::tape::tapegateway::WRITE);
  volumeMsg.setLabel(m_vmgrTapeInfo.lbltype);
  volumeMsg.setMountTransactionId(m_volReqId);
  volumeMsg.setAggregatorTransactionId(volumeRequest.aggregatorTransactionId());
  volumeMsg.setDensity(m_vmgrTapeInfo.density);

  // Send the volume message to the tapebridge
  connection.sendObject(volumeMsg);

  Helper::displaySentMsgIfDebug(volumeMsg, m_cmdLine.debugSet);
}


//------------------------------------------------------------------------------
// performTransfer
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::performTransfer()
  throw (castor::exception::Exception) {

  std::ostream &os = std::cout;

  // Query the VMGR for information about the tape again in order to get
  // the latest number of files written to the tape
  //
  // Please note that the tapebridged daemon will check again that writetp is
  // not overwriting any files written by the CASTOR stagers that succesfully
  // updated the VMGR file counters
  vmgrQueryTape();
  m_nextTapeFseq = m_vmgrTapeInfo.nbfiles + 1;

  {
    time_t now = time(NULL);
    utils::writeTime(os, now, TIMEFORMAT);
  }
  os << " Writing to tape " << m_cmdLine.vid <<
    " starting at tape-file sequence-number " << m_nextTapeFseq << std::endl
    << std::endl;

  // Spin in the wait for and dispatch message loop until there is no more work
  while(waitForAndDispatchMessage()) {
    // Do nothing
  }

  const uint64_t nbMigrateRequests     = m_fileTransactionId - 1;
  const uint64_t nbIncompleteTransfers = m_pendingFileTransfers.size();

  {
    time_t now = time(NULL);
    utils::writeTime(os, now, TIMEFORMAT);
  }
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
// handleFilesMigrateListRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleFilesToMigrateListRequest(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FilesToMigrateListRequest *msg = NULL;

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

      // Notify the tapebridge about the exception and rethrow
      sendEndNotificationErrorReport(msg->aggregatorTransactionId(),
        ex.code(), ex.getMessage().str(), sock);
      throw(ex);
    }

    // Create a FilesToMigrateList message for the tapebridge
    std::auto_ptr<tapegateway::FileToMigrateStruct> file(
      new tapegateway::FileToMigrateStruct());
    file->setFileTransactionId(m_fileTransactionId);
    file->setNshost("tpcp");
    file->setFileid(0);
    file->setFileSize(statBuf.st_size);
    file->setLastKnownFilename(filename);
    file->setLastModificationTime(statBuf.st_mtime);
    file->setUmask(RTCOPYCONSERVATIVEUMASK);
    file->setPath(filename);
    file->setPositionCommandCode(tapegateway::TPPOSIT_FSEQ);
    file->setFseq(m_nextTapeFseq++);

    tapegateway::FilesToMigrateList fileList;
    fileList.setMountTransactionId(m_volReqId);
    fileList.setAggregatorTransactionId(msg->aggregatorTransactionId());
    fileList.addFilesToMigrate(file.release());

    // Update the map of current file transfers and increment the file
    // transaction ID
    {
      m_pendingFileTransfers[m_fileTransactionId] = filename;
      m_fileTransactionId++;
    }

    // Send the FilesToMigrateList message to the tapebridge
    sock.sendObject(fileList);

    Helper::displaySentMsgIfDebug(fileList, m_cmdLine.debugSet);

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

    // Create the NoMoreFiles message for the tapebridge
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setMountTransactionId(m_volReqId);
    noMore.setAggregatorTransactionId(msg->aggregatorTransactionId());

    // Send the NoMoreFiles message to the tapebridge
    sock.sendObject(noMore);

    Helper::displaySentMsgIfDebug(noMore, m_cmdLine.debugSet);
  }

  return true;
}


//------------------------------------------------------------------------------
// handleFileMigrationReportList
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleFileMigrationReportList(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::FileMigrationReportList *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  if(msg->fseqSet()) {
    castor::exception::Exception ex(ENOTSUP);

    ex.getMessage() <<
      "Failed to handle FileMigrationReportList message"
      ": This version of writetp does not support the tapebridged daemon"
      " setting the tape-file sequence-number in the VMGR";

    throw ex;
  }

  handleSuccessfulMigrations(msg->aggregatorTransactionId(),
    msg->successfulMigrations(), sock);

  TpcpCommand::handleFailedTransfers(msg->failedMigrations());

  // Create the NotificationAcknowledge message for the tapebridge
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);
  acknowledge.setAggregatorTransactionId(msg->aggregatorTransactionId());

  // Send the NotificationAcknowledge message to the tapebridge
  sock.sendObject(acknowledge);

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);

  return true;
}


//------------------------------------------------------------------------------
// handleSuccessfulMigrations
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::handleSuccessfulMigrations(
  const uint64_t tapebridgeTransId,
  const std::vector<tapegateway::FileMigratedNotificationStruct*> &files,
  castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {
  for(std::vector<tapegateway::FileMigratedNotificationStruct*>::const_iterator
    itor = files.begin(); itor != files.end(); itor++) {

    if(NULL == *itor) {
      TAPE_THROW_CODE(EBADMSG,
        "Pointer to FileMigratedNotificationStruct is NULL");
    }

    handleSuccessfulMigration(tapebridgeTransId, *(*itor), sock);
  }
}


//------------------------------------------------------------------------------
// handleSuccessfulMigration
//------------------------------------------------------------------------------
void castor::tape::tpcp::WriteTpCommand::handleSuccessfulMigration(
  const uint64_t tapebridgeTransId,
  const tapegateway::FileMigratedNotificationStruct &file,
  castor::io::AbstractSocket &sock) throw(castor::exception::Exception) {
  // Check the file transaction ID
  {
    FileTransferMap::iterator itor =
      m_pendingFileTransfers.find(file.fileTransactionId());

    // If the fileTransactionId is unknown
    if(itor == m_pendingFileTransfers.end()) {
      std::stringstream oss;

      oss <<
        "Received unknown file transaction ID from the tapebridge"
        ": fileTransactionId=" << file.fileTransactionId();

      sendEndNotificationErrorReport(tapebridgeTransId, EBADMSG,
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
       " size=" << file.fileSize() <<
       " checksum=0x" << std::hex << file.checksum() << std::dec << std::endl;

    // The file has been transfer so remove it from the map of pending
    // transfers
    m_pendingFileTransfers.erase(itor);
  }

  // Update the count of successfull recalls
  m_nbMigratedFiles++;
}


//------------------------------------------------------------------------------
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleEndNotification(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handleEndNotificationErrorReport(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotificationErrorReport(obj, sock);
}


//------------------------------------------------------------------------------
// handlePingNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::WriteTpCommand::handlePingNotification(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handlePingNotification(obj,sock);
}
