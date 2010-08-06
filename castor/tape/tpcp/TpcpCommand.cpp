/******************************************************************************
 *                 castor/tape/tpcp/TpcpCommand.cpp
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
#include "castor/PortNumbers.hpp"
#include "castor/System.hpp"
#include "castor/exception/Internal.hpp" 
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/PingNotification.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Helper.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/tpcp/StreamOperators.hpp"
#include "castor/tape/tpcp/TapeFileSequenceParser.hpp"
#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"
#include "castor/tape/tpcp/TpcpCommand.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Cgetopt.h"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/Cupv_api.h"
#include "h/Cupv_constants.h"
#include "h/serrno.h"
#include "h/vdqm_api.h"
#include "h/vmgr_api.h"

#include <ctype.h>
#include <exception>
#include <getopt.h>
#include <iostream>
#include <list>
#include <stdint.h>
#include <string.h>
#include <sys/unistd.h>
#include <time.h>
#include <unistd.h>
#include <poll.h>
 
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <memory>

//------------------------------------------------------------------------------
// vmgr_error_buffer
//------------------------------------------------------------------------------
char castor::tape::tpcp::TpcpCommand::vmgr_error_buffer[VMGRERRORBUFLEN];


//------------------------------------------------------------------------------
// s_receivedSigint
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TpcpCommand::s_receivedSigint = false;


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::TpcpCommand(const char *const programName)
  throw () :
  m_programName(programName),
  m_userId(getuid()),
  m_groupId(getgid()),
  m_callbackSock(false),
  m_gotVolReqId(false),
  m_volReqId(0),
  m_fileTransactionId(1) {

  // Prepare the SIGINT action handler structure ready for sigaction()
  m_sigintAction.sa_handler = &sigintHandler;
  if(sigfillset(&m_sigintAction.sa_mask) < 0) { // Mask all signals
    const int savedErrno = errno;

    castor::exception::Exception ex(savedErrno);

    TAPE_THROW_CODE(savedErrno,
      "Failed to initialize signal mask using sigfillset"
      ": " << sstrerror(savedErrno));
  }
  m_sigintAction.sa_flags = 0; // No flags

  // Set the SIGINT signal handler
  if(sigaction(SIGINT, &m_sigintAction, 0) < 0){
    const int savedErrno = errno;

    TAPE_THROW_CODE(savedErrno,
      "Failed to set the SIGINT signal handler using sigaction"
      ": " << sstrerror(savedErrno));
  }

  utils::setBytes(m_vmgrTapeInfo, '\0');
  utils::setBytes(m_dgn, '\0');
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::~TpcpCommand() throw () {
  // Do nothing
}



//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::main(const int argc, char **argv) throw() {

  // Parse the command line
  try {
    parseCommandLine(argc, argv);
  } catch (castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to parse the command-line:\n\n"
      << ex.getMessage().str() << std::endl
      << std::endl;
    usage(std::cerr);
    std::cerr << std::endl;
    return 1;
  }

  // If debug, then display parsed command-line arguments
  if(m_cmdLine.debugSet) {
    std::cout <<
      "Parsed command-line = " << m_cmdLine << std::endl;
  }

  // Display usage message and exit if help option found on command-line
  if(m_cmdLine.helpSet) {
    std::cout << std::endl;
    usage(std::cout);
    std::cout << std::endl;
    return 0;
  }

  // Abort if the requested action type is not yet supportd
  if(m_cmdLine.action != Action::read  &&
     m_cmdLine.action != Action::write &&
     m_cmdLine.action != Action::dump) {
     castor::exception::Internal ex;

     ex.getMessage() << "Unknown action";

     throw ex;
  }

  // Execute the command
  try {
    executeCommand();
  } catch(castor::exception::Exception &ex) {
    displayErrorMsgCleanUpAndExit(ex.getMessage().str().c_str());
  } catch(std::exception &se) {
    displayErrorMsgCleanUpAndExit(se.what());
  } catch(...) {
    displayErrorMsgCleanUpAndExit("Caught unknown exception");
  }

  return 0; // Success
}


//------------------------------------------------------------------------------
// displayErrorMsgCleanUpAndExit
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::displayErrorMsgCleanUpAndExit(
  const char *msg) throw() {

  // Display error message
  {
    const time_t now = time(NULL);
    utils::writeTime(std::cerr, now, TIMEFORMAT);
    std::cerr <<
      " " << m_programName << " failed"
      ": " << msg << std::endl;
  }

  // Clean up
  if(m_gotVolReqId) {
    const time_t now = time(NULL);
    utils::writeTime(std::cerr, now, TIMEFORMAT);
    std::cerr <<
      " Deleting volume request with ID = " << m_volReqId << std::endl;

    try {
      deleteVdqmVolumeRequest();
    } catch(castor::exception::Exception &ex) {

      const time_t now = time(NULL);
  
      utils::writeTime(std::cerr, now, TIMEFORMAT);
      std::cerr << " Failed to delete VDQM volume request: "
         << ex.getMessage().str() << std::endl;
    }
  }

  exit(1); // Error
}


//------------------------------------------------------------------------------
// executeCommand
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::executeCommand() {

  // Get the local hostname
  gethostname(m_hostname, sizeof(m_hostname));    

  // Check if the hostname of the machine is set to "localhost"
  if(strcmp(m_hostname, "localhost") == 0) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      m_programName << " cannot be ran on a machine where hostname is set to "
      "\"localhost\"";
    throw ex;
  }
    
  // Get the Current Working Directory
  getcwd(m_cwd, PATH_MAX);
  if(m_cwd == NULL){
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      m_programName << " cannot be ran on a machine where the current working "
      "directory is not been set";
    throw ex;
  }

  // This command cannot be ran as root
  if(m_userId == 0 && m_groupId == 0) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      m_programName << " cannot be ran as root";
    throw ex;
  }

  // Set the VMGR error buffer so that the VMGR does not write errors to
  // stderr
  vmgr_error_buffer[0] = '\0';
  if(vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      "Failed to set VMGR error buffer";
    throw ex;
  }

  // Fill the list of filenames to be processed.
  //
  // The list of filenames will either come from the command-line arguments
  // or (exclusive or) from a "filelist" file specified with the
  // "-f, --filelist" option.
  if(m_cmdLine.fileListSet) {
    // Parse the "filelist" file into the list of filenames to be
    // processed
    utils::parseFileList(m_cmdLine.fileListFilename.c_str(),
      m_filenames);
  } else {
    // Copy the command-line argument filenames into the list of filenames
    // to be processed
    for(FilenameList::const_iterator
      itor=m_cmdLine.filenames.begin();
      itor!=m_cmdLine.filenames.end(); itor++) {
      m_filenames.push_back(*itor);
    }
  }

  //check the format of the filename: hostname:/filepath/filename
  checkFilenameFormat();

  // If debug, then display the list of files to be processed by the action
  // handlers
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "Filenames to be processed = " << m_filenames << std::endl;
  }

  // Set the iterator pointing to the next RFIO filename to be processed
  m_filenameItor = m_filenames.begin();

  if(m_cmdLine.action == Action::read) {

    // Determine the number of tape files based on the sequence of tape
    // file sequence numbers they would produce
    //
    // Note that the number maybe infinite if the user has requested to read
    // until the end of the tape
    TapeFseqRangeListSequence seq(&m_cmdLine.tapeFseqRanges);

    // Throw an exception if the number of tape files is finite and does not
    // match the number of RFIO file names
    if(seq.isFinite() && seq.totalSize() != m_filenames.size()) {
      castor::exception::InvalidArgument ex;

      ex.getMessage() <<
        "The number of tape files does not match the number of RFIO "
        "filenames"
        ": Number of tape files=" << seq.totalSize() <<
        " Number of RFIO filenames=" << m_filenames.size();

      throw ex;
    }

    // RFIO stat the directory to which the first file to be recalled file
    // will be written to in order to check the RFIO daemon is running
    {
      FilenameList::const_iterator itor = m_filenames.begin();

      // Sanity check - there should be at least one file to be migrated
      if(itor == m_filenames.end()) {
        TAPE_THROW_EX(exception::Internal,
          ": List of filenames to be processed is unexpectedly empty");
      }

      const std::string &filename = *itor;
      std::string filepath(filename.substr(0, filename.find_last_of("/")+1));

      // Command-line user feedback
      std::ostream &os = std::cout;
      time_t       now = time(NULL);

      utils::writeTime(os, now, TIMEFORMAT);
      os << " RFIO stat the directory of the first file \""
         << filepath << "\"" << std::endl;

      // Perform the RFIO stat
      struct stat64 statBuf;
      rfioStat(filepath.c_str(), statBuf);

      // Test if the filepath is not a directory
      if( (statBuf.st_mode & S_IFMT) != S_IFDIR ){
        castor::exception::Exception ex(ECANCELED);
        ex.getMessage() <<
          ": Invalid RFIO filename syntax"
          ": Filepath must identify a regular directory"
          ": filepath=\"" << filepath.c_str() <<"\"";

        throw ex;
      }
    }
  } // if(m_cmdLine.action == Action::read)

  if(m_cmdLine.action == Action::write) {

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
  } // if(m_cmdLine.action == Action::write)

  // Get information about the tape to be used from the VMGR
  try {
    const int side = 0;
    vmgrQueryTape(m_cmdLine.vid, side);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ECANCELED);

    std::ostream &os = ex2.getMessage();
    os << "Failed to query the VMGR about tape: VID = "
       << m_cmdLine.vid;

    // If the tape does not exist
    if(ex.code() == ENOENT) {
      os << ": Tape does not exist";
    } else {
      os << ": " << ex.getMessage().str();
    }

    throw ex2;
  }

  // If debug, then display the tape information retrieved from the VMGR
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "vmgr_tape_info from the VMGR = " <<  m_vmgrTapeInfo << std::endl;

    os << "DGN from the VMGR =\"" << m_dgn << "\"" << std::endl;
  }

  // Check that the VID returned in the VMGR tape information matches that of
  // the requested tape
  if(strcmp(m_cmdLine.vid, m_vmgrTapeInfo.vid) != 0) {
     castor::exception::Exception ex(ECANCELED);
     std::ostream &os = ex.getMessage();

     os <<
       "VID in tape information retrieved from VMGR does not match that of "
       "the requested tape"
       ": Request VID = " << m_cmdLine.vid <<
       " VID returned from VMGR = " << m_vmgrTapeInfo.vid;

     throw ex;
  }

  // If writing to tape then check that the user has permission to write to
  // the tape
  if(m_cmdLine.action == Action::write) {
    checkUserHasTapeWritePermission(m_vmgrTapeInfo.poolname, m_userId,
      m_groupId, m_hostname);
  }

  // Check the tape is available
  if(m_vmgrTapeInfo.status & DISABLED ||
     m_vmgrTapeInfo.status & EXPORTED ||
     m_vmgrTapeInfo.status & ARCHIVED) {

     castor::exception::Exception ex(ECANCELED);
     std::ostream &os = ex.getMessage();

     os << "Tape is not available: Tape is: ";

     if(m_vmgrTapeInfo.status & DISABLED) os << " DISABLED";
     if(m_vmgrTapeInfo.status & EXPORTED) os << " EXPORTED";
     if(m_vmgrTapeInfo.status & ARCHIVED) os << " ARCHIVED";

     throw ex;
  }

  // Check if the access mode of the tape is compatible with the action to be
  // performed by tpcp
  if(m_cmdLine.action == Action::write &&
    m_vmgrTapeInfo.status & TAPE_RDONLY) {

    castor::exception::Exception ex(ECANCELED);

     ex.getMessage() << "Tape cannot be written to"
       ": Tape marked as TAPE_RDONLY";

     throw ex;
  }

  // Setup the tapebridge callback socket
  setupCallbackSock();

  // If debug, then display a textual description of the tapebridge callback
  // socket
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "Tapebridge callback socket details = ";
    net::writeSockDescription(os, m_callbackSock.socket());
    os << std::endl;
  }

  // Send the request for a drive to the VDQM
  {
    const int mode = m_cmdLine.action == Action::write ?
      WRITE_ENABLE : WRITE_DISABLE;
    char *const server = m_cmdLine.serverSet ?
      m_cmdLine.server : NULL;
    requestDriveFromVdqm(mode, server);
  }

  // Command-line user feedback
  {
    std::ostream &os = std::cout;
    time_t       now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os << " Waiting for a drive: Volume request ID = " << m_volReqId
       << std::endl;
  }

  // Socket file descriptor for a callback connection from the tapebridge
  int connectionSockFd = 0;

  // Wait for a callback connection from the tapebridge
  {
    bool   waitForCallback = true;
    time_t timeout         = WAITCALLBACKTIMEOUT;
    while(waitForCallback) {
      try {
        connectionSockFd = net::acceptConnection(m_callbackSock.socket(),
          WAITCALLBACKTIMEOUT);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {

        // Command-line user feedback
        std::ostream &os = std::cout;
        time_t       now = time(NULL);

        utils::writeTime(os, now, TIMEFORMAT);
        os <<
          " Waited " << WAITCALLBACKTIMEOUT << " seconds.  "
          "Continuing to wait." <<  std::endl;

        // Wait again for the default timeout
        timeout = WAITCALLBACKTIMEOUT;
        
      } catch(castor::exception::TapeNetAcceptInterrupted &ix) {

        // If a SIGINT signal was received (control-c)
        if(s_receivedSigint) {

          castor::exception::Exception ex(ECANCELED);
          ex.getMessage() << "Received SIGNINT";

          throw(ex);

        // Else received a signal other than SIGINT
        } else {

          // Wait again for the remaining amount of time
          timeout = ix.remainingTime();
        }
      }
    }
  }

  // Command-line user feedback
  {
    std::ostream &os = std::cout;
    time_t       now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os << " Selected tape server is ";

    char hostName[net::HOSTNAMEBUFLEN];

    net::getPeerHostName(connectionSockFd, hostName);

    os << hostName << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket callbackConnectionSock(connectionSockFd);

  // Read in the object sent by the tapebridge
  std::auto_ptr<castor::IObject> firstObj(callbackConnectionSock.readObject());

  switch (firstObj->type()) {
  case OBJ_VolumeRequest:
    // Do nothing as this is the type of message we expect in the
    // good-day scenario
    break;

  case OBJ_EndNotificationErrorReport:
    {
      // Cast the object to its type
      tapegateway::EndNotificationErrorReport &endNotificationErrorReport =
        dynamic_cast<tapegateway::EndNotificationErrorReport&>(
          *(firstObj.get()));

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Tape-bridge reported an error"
        ": " << endNotificationErrorReport.errorMessage();
      throw ex;
    }
  default:
    {
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        "Received the wrong type of object from the tapebridge" <<
        ": Actual=" << utils::objectTypeToString(firstObj->type()) <<
        " Expected=VolumeRequest or EndNotificationErrorReport";
      throw ex;
    }
  } //switch (firstObj->type())

  // Cast the object to its type
  tapegateway::VolumeRequest &volumeRequest =
    dynamic_cast<tapegateway::VolumeRequest&>(*(firstObj.get()));

  Helper::displayRcvdMsgIfDebug(volumeRequest, m_cmdLine.debugSet);

  {
    std::ostream &os = std::cout;
    time_t       now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os << " Selected drive unit is " << volumeRequest.unit() << std::endl;
  }

  // Check the volume request ID of the VolumeRequest object matches that of
  // the reply from the VDQM when the drive was requested
  if(volumeRequest.mountTransactionId() != (uint64_t)m_volReqId) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "Received the wrong mount transaction ID from the tapebridge"
      << ": Actual=" << volumeRequest.mountTransactionId()
      << " Expected=" <<  m_volReqId;

    throw ex;
  }

  // Create the volume message for the tapebridge
  castor::tape::tapegateway::Volume volumeMsg;
  volumeMsg.setVid(m_vmgrTapeInfo.vid);
  switch(m_cmdLine.action.value()) {
  case Action::READ:
    volumeMsg.setClientType(castor::tape::tapegateway::READ_TP);
    volumeMsg.setMode(castor::tape::tapegateway::READ);
    break;
  case Action::WRITE:
    volumeMsg.setClientType(castor::tape::tapegateway::WRITE_TP);
    volumeMsg.setMode(castor::tape::tapegateway::WRITE);
    break;
  case Action::DUMP:
    volumeMsg.setClientType(castor::tape::tapegateway::DUMP_TP);
    volumeMsg.setMode(castor::tape::tapegateway::DUMP);
    break;
  default:
    TAPE_THROW_EX(castor::exception::Internal,
      ": Unknown action type: value=" << m_cmdLine.action.value());
  }
  volumeMsg.setLabel(m_vmgrTapeInfo.lbltype);
  volumeMsg.setMountTransactionId(m_volReqId);
  volumeMsg.setAggregatorTransactionId(
    volumeRequest.aggregatorTransactionId());
  volumeMsg.setDensity(m_vmgrTapeInfo.density);

  // Send the volume message to the tapebridge
  callbackConnectionSock.sendObject(volumeMsg);

  Helper::displaySentMsgIfDebug(volumeMsg, m_cmdLine.debugSet);

  // Close the connection to the tapebridge
  callbackConnectionSock.close();

  // Perform the transfer, either READ, WRITE or DUMP
  try {
    performTransfer();
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ECANCELED);

    ex2.getMessage() <<
      "Failed to perform " << m_cmdLine.action <<
      ": " << ex.getMessage().str();

    throw ex2;
  }
}


//------------------------------------------------------------------------------
// vmgrQueryTape
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::vmgrQueryTape(
  char (&vid)[CA_MAXVIDLEN+1], const int side)
  throw (castor::exception::Exception) {

  serrno=0;
  const int rc = vmgr_querytape(vid, side, &m_vmgrTapeInfo, m_dgn);
  const int savedSerrno = serrno;

  if(rc != 0) {
    TAPE_THROW_CODE(savedSerrno,
      ": Failed call to vmgr_querytape()"
      ": " << sstrerror(savedSerrno));
  }
}


//------------------------------------------------------------------------------
// setupCallbackSock
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::setupCallbackSock()
  throw(castor::exception::Exception) {

  const unsigned short lowPort = utils::getPortFromConfig(
    "TAPESERVERCLIENT", "LOWPORT", TAPEBRIDGECLIENT_LOWPORT);
  const unsigned short highPort = utils::getPortFromConfig(
    "TAPESERVERCLIENT", "HIGHPORT", TAPEBRIDGECLIENT_HIGHPORT);

  // Bind the tapebridge callback socket
  m_callbackSock.bind(lowPort, highPort);
  m_callbackSock.listen();
}


//------------------------------------------------------------------------------
// requestDriveFromVdqm 
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::requestDriveFromVdqm(
  const int accessMode, char *const tapeServer)
  throw(castor::exception::Exception) {

  // Get the port number and IP of the callback port
  unsigned short port = 0;
  unsigned long  ip   = 0;
  m_callbackSock.getPortIp(port, ip);

  int      rc          = 0;
  int      savedSerrno  = 0;
  vdqmnw_t *nw         = NULL;
  int      *reqID      = &m_volReqId;
  char     *VID        = m_cmdLine.vid;
  char     *dgn        = m_dgn;
  char     *server     = tapeServer;
  char     *unit       = NULL;
  int      mode        = accessMode;
  int      client_port = port;

  // Connect to the VDQM
  rc = vdqm_Connect(&nw);
  savedSerrno = serrno;

  // If successfully connected
  if(rc != -1) {

    // Ask the VDQM to create a request for a drive with an tapebridge
    rc = vdqm_CreateRequestForAggregator(nw, reqID, VID, dgn, server, unit,
      mode, client_port);
    savedSerrno = serrno;

    // If the request was sucessfully created
    if(rc != -1) {

      // Ask the VDQM to queue the newly created request
      rc = vdqm_QueueRequestForAggregator(nw);
      savedSerrno = serrno;
    }

    // If the request was successfully queued
    if(rc != -1) {

      // Record the fact
      m_gotVolReqId = true;
    }

    // Disconnect from the VDQM
    rc = vdqm_Disconnect(&nw);
    savedSerrno = serrno;
  }

  // Throw an exception if there was an error
  if(rc == -1) {
    castor::exception::Exception ex(savedSerrno);

    ex.getMessage() << sstrerror(savedSerrno);

    throw ex;
  }
}


//------------------------------------------------------------------------------
// waitForAndDispatchMessage
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TpcpCommand::waitForAndDispatchMessage()
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the tapebridge
  int connectionSockFd = 0;

  // Wait for a callback connection from the tapebridge
  {
    bool   waitForCallback = true;
    time_t timeout         = WAITCALLBACKTIMEOUT;
    while(waitForCallback) {
      try {
        connectionSockFd = net::acceptConnection(m_callbackSock.socket(),
          timeout);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {

        // Command-line user feedback
        std::ostream &os = std::cout;
        time_t       now = time(NULL);

        utils::writeTime(os, now, TIMEFORMAT);
        os <<
          " Waited " << WAITCALLBACKTIMEOUT << " seconds.  "
          "Continuing to wait." <<  std::endl;

        // Wait again for the default timeout
        timeout = WAITCALLBACKTIMEOUT;

      } catch(castor::exception::TapeNetAcceptInterrupted &ix) {

         // If a SIGINT signal was received (control-c)
         if(s_receivedSigint) {
          castor::exception::Exception ex(ECANCELED);

          ex.getMessage() << "Received SIGNINT";
          throw(ex);

        // Else received a signal other than SIGINT
        } else {

          // Wait again for the remaining amount of time
          timeout = ix.remainingTime();
        }
      }
    }
  }

  // If debug, then display a textual description of the tapebridge
  // callback connection
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "Tapebridge connection = ";
    net::writeSockDescription(os, connectionSockFd);
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket sock(connectionSockFd);

  // Read in the message sent by the tapebridge
  std::auto_ptr<castor::IObject> obj(sock.readObject());

  // If debug, then display the type of message received from the tapebridge
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "Received tapebridge message of type = "
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
        ": actual=" << utils::objectTypeToString(obj->type()) <<
        " expected=Subclass of GatewayMessage";

      const uint64_t tapebridgeTransactionId = 0; // Unknown transaction ID
      sendEndNotificationErrorReport(tapebridgeTransactionId, SEINTERNAL,
        oss.str(), sock);

      TAPE_THROW_EX(castor::exception::Internal, oss.str());
    }

    // Check the mount transaction ID
    if(msg->mountTransactionId() != (uint64_t)m_volReqId) {
      std::stringstream oss;

      oss <<
        "Mount transaction ID mismatch" <<
        ": actual=" << msg->mountTransactionId() <<
        " expected=" << m_volReqId;

      sendEndNotificationErrorReport(msg->aggregatorTransactionId(), EBADMSG,
        oss.str(), sock);

      castor::exception::Exception ex(EBADMSG);
      ex.getMessage() << oss.str();
      throw ex;
    }
  }

  // Find the message type's corresponding handler
  MsgHandlerMap::const_iterator itor = m_msgHandlers.find(obj->type());
  if(itor == m_msgHandlers.end()) {
    std::stringstream oss;

    oss <<
      "Received unexpected tapebridge message"
      ": Message type = " << utils::objectTypeToString(obj->type());

    const uint64_t tapebridgeTransactionId = 0; // Unknown transaction ID
    sendEndNotificationErrorReport(tapebridgeTransactionId, EBADMSG, oss.str(),
      sock);

    TAPE_THROW_CODE(EBADMSG,
         ": Received unexpected tapebridge message "
         ": Message type = " << utils::objectTypeToString(obj->type()));
  }
  const AbstractMsgHandler &handler = *itor->second;

  // Invoke the handler
  const bool moreWork = handler(obj.get(), sock);

  // Close the tapebridge callback connection
  sock.close();

  return moreWork;
}


//------------------------------------------------------------------------------
// handlePingNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TpcpCommand::handlePingNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::PingNotification *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

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
// handleEndNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TpcpCommand::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::EndNotification *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  // Create the NotificationAcknowledge message for the tapebridge
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);
  acknowledge.setAggregatorTransactionId(msg->aggregatorTransactionId());

  // Send the NotificationAcknowledge message to the tapebridge
  sock.sendObject(acknowledge);

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);

  return false;
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::TpcpCommand::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::EndNotificationErrorReport *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  // Command-line user feedback
  {
    std::ostream &os = std::cout;
    const time_t now = time(NULL);

    utils::writeTime(os, now, TIMEFORMAT);
    os <<
      " Tapebridge encountered the following error:" << std::endl <<
      std::endl <<
      "Error code        = "   << msg->errorCode()            << std::endl <<
      "Error code string = \"" << sstrerror(msg->errorCode()) << "\"" <<
      std::endl <<
      "Error message     = \"" << msg->errorMessage() << "\"" << std::endl <<
      std::endl;
  }

  // Create the NotificationAcknowledge message for the tapebridge
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);
  acknowledge.setAggregatorTransactionId(msg->aggregatorTransactionId());

  // Send the NotificationAcknowledge message to the tapebridge
  sock.sendObject(acknowledge);

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);

  return false;
}


//------------------------------------------------------------------------------
// acknowledgeEndOfSession
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::acknowledgeEndOfSession()
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the tapebridge
  int connectionSockFd = 0;

  // Wait for a callback connection from the tapebridge
  {
    bool   waitForCallback = true;
    time_t timeout         = WAITCALLBACKTIMEOUT;
    while(waitForCallback) {
      try {
        connectionSockFd = net::acceptConnection(m_callbackSock.socket(),
          timeout);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {

        // Command-line user feedback
        std::ostream &os = std::cout;
        time_t       now = time(NULL);

        utils::writeTime(os, now, TIMEFORMAT);
        os <<
          " Waited " << WAITCALLBACKTIMEOUT << " seconds.  "
          "Continuing to wait." <<  std::endl;

        // Wait again for the default timeout
        timeout = WAITCALLBACKTIMEOUT;

      } catch(castor::exception::TapeNetAcceptInterrupted &ix) {

        // If a SIGINT signal was received (control-c)
        if(s_receivedSigint) {
          castor::exception::Exception ex(ECANCELED);

          ex.getMessage() <<
           ": Received SIGNINT";

          throw(ex);

        // Else received a signal other than SIGINT
        } else {

          // Wait again for the remaining amount of time
          timeout = ix.remainingTime();
        }
      }
    }
  }

  // If debug, then display a textual description of the tapebridge
  // callback connection
  if(m_cmdLine.debugSet) {
    std::ostream &os = std::cout;

    os << "Tapebridge connection = ";
    net::writeSockDescription(os, connectionSockFd);
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket sock(connectionSockFd);

  // Read in the object sent by the tapebridge
  std::auto_ptr<castor::IObject> obj(sock.readObject());

  // Pointer to the received object with the object's type
  tapegateway::EndNotification *endNotification = NULL;

  castMessage(obj.get(), endNotification, sock);
  Helper::displayRcvdMsgIfDebug(*endNotification, m_cmdLine.debugSet);

  // Create the NotificationAcknowledge message for the tapebridge
  castor::tape::tapegateway::NotificationAcknowledge acknowledge;
  acknowledge.setMountTransactionId(m_volReqId);
  acknowledge.setAggregatorTransactionId(
    endNotification->aggregatorTransactionId());

  // Send the volume message to the tapebridge
  sock.sendObject(acknowledge);

  // Close the connection to the tapebridge
  sock.close();

  Helper::displaySentMsgIfDebug(acknowledge, m_cmdLine.debugSet);
}


//------------------------------------------------------------------------------
// sendEndNotificationErrorReport
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::sendEndNotificationErrorReport(
  const uint64_t             tapebridgeTransactionId,
  const int                  errorCode,
  const std::string          &errorMessage,
  castor::io::AbstractSocket &sock)
  throw() {

  try {
    // Create the message
    tapegateway::EndNotificationErrorReport errorReport;
    errorReport.setAggregatorTransactionId(tapebridgeTransactionId);
    errorReport.setErrorCode(errorCode);
    errorReport.setErrorMessage(errorMessage);

    // Send the message to the tapebridge
    sock.sendObject(errorReport);

    Helper::displaySentMsgIfDebug(errorReport, m_cmdLine.debugSet);
  } catch(...) {
    // Do nothing
  }
}


//------------------------------------------------------------------------------
// sigintHandler
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::sigintHandler(int) {
  s_receivedSigint = true;
}


//------------------------------------------------------------------------------
// deleteVdqmVolumeRequest
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::deleteVdqmVolumeRequest()
  throw(castor::exception::Exception) {

  const int rc = vdqm_DelVolumeReq(NULL, m_volReqId, m_vmgrTapeInfo.vid, m_dgn,
    NULL, NULL, 0);
  const int savedSerrno = serrno;

  if(rc < 0) {
    TAPE_THROW_CODE(savedSerrno,
      "Failed call to vdqm_DelVolumeReq()"
      ": " << sstrerror(savedSerrno));
  }
}


//------------------------------------------------------------------------------
// checkFilenameFormat
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::checkFilenameFormat()
  throw(castor::exception::Exception) {

  FilenameList::iterator itor = m_filenames.begin();
  // local string containing the hostname + ":"
  std::string hostname(m_hostname);
  hostname.append(":");

  size_t firstPos;
  while(itor!=m_filenames.end()) {

   std::string &line = *itor;

   // check if the filename ends with '.' or '/'
   const char *characters = "/.";
   std::string::size_type end = line.find_last_not_of(characters);

   if(end == std::string::npos || end != line.length()-1) {

    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
           ": Invalid RFIO filename syntax"
           ": Filename must identify a regular file"
           ": filename=\"" << line <<"\"";

      throw ex;
   } 
   
   firstPos = line.find(":/");

   // if there are 0 ":/" --> (is a local file) 
   if(firstPos == std::string::npos){
     if(line.find_first_of("/") != 0){
       // Prefix it with the CWD
       line.insert(0, "/");
       line.insert(0, m_cwd);
     }
     // Prefix it with the Hostname
     line.insert(0, hostname);

   } else {
       // if there is at least 1 ":/" -->check the "hostname" entered by the 
       // user (note :/ can be part of the filepath)
       std::string str = line.substr(0, firstPos);
       if(str.empty()){
         castor::exception::Exception ex(ECANCELED);
         ex.getMessage() <<
           ": Invalid RFIO filename syntax"
           ": Missing hostname before ':/'"
           ": filename=\"" << line <<"\"";

         throw ex;
       }
       // if file hostamane == "localhost" or "127.0.0.1"  
       // --> replace it with hostname
       if (str == "localhost" || str == "127.0.0.1"){

         line.replace(0, firstPos+1, hostname);
       }
   } 

  ++itor;
  }//for(itor=m_cmdLine.filenames.begin()...
}


//------------------------------------------------------------------------------
// rfioStat
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::rfioStat(const char *const path,
  struct stat64 &statBuf) throw(castor::exception::Exception) {

  // rfio_stat64 is a smart oparation, in case of a local file, it do not use
  // the rfio's stat, but the local stat. To forse the use of rfio's stat we
  // convert the hostname (if equal to local hostname) into the local IP address
  std::string filename (path);

  if(filename.find(m_hostname)!= std::string::npos){
    const int firstPos = filename.find(":/");
    struct hostent *tmpHost;
    tmpHost = gethostbyname(m_hostname);
    const std::string ip ( inet_ntoa(*((struct in_addr *)tmpHost->h_addr_list[0])) );
    filename.replace(0, firstPos, ip);
    }

  //const int rc = rfio_stat64((char *)path, &statBuf);
  const int rc = rfio_stat64((char *)filename.c_str(), &statBuf);
  const int savedSerrno = rfio_serrno();

  if(rc != 0) {
    // In case of failure, rfio_stat64 can set:
    // serrno     if error from support routines
    // errno      if error from system calls
    // rfio_errno if error from remote host (if remote host runs a different
    //            OS, then it will return a locally unknown error code)
    //
    // rfio_serrno() Returns the error taking into account what rfio_stat64 can
    //               set.
    // rfio_serror() Returns the error string matching the error code (if
    //               the error is from a remote host, then this function
    //               connects to the host and asks for the maching string)

    const char *err_msg = rfio_serror();

    castor::exception::Exception ex(savedSerrno);

    ex.getMessage() << "Failed to rfio_stat64 \"" << path << "\""
      ": " << err_msg;

    throw(ex);
  }
}


//------------------------------------------------------------------------------
// checkUserHasTapeWritePermission
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::checkUserHasTapeWritePermission(
  const char *const poolName, const uid_t userId, const gid_t groupId,
  const char *const sourceHost) throw(castor::exception::PermissionDenied) {

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
