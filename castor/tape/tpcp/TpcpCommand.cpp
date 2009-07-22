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
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Helper.hpp"
#include "castor/tape/tpcp/StreamHelper.hpp"
#include "castor/tape/tpcp/StreamOperators.hpp"
#include "castor/tape/tpcp/TapeFileSequenceParser.hpp"
#include "castor/tape/tpcp/TpcpCommand.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Cgetopt.h"
#include "h/common.h"
#include "h/Ctape_constants.h"
#include "h/serrno.h"
#include "h/vdqm_api.h"
#include "h/vmgr_api.h"

#include <ctype.h>
#include <getopt.h>
#include <iostream>
#include <list>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <poll.h>


//------------------------------------------------------------------------------
// vmgr_error_buffer
//------------------------------------------------------------------------------
char castor::tape::tpcp::TpcpCommand::vmgr_error_buffer[VMGRERRORBUFLEN];


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::TpcpCommand() throw () :
  m_callbackSock(false),
  m_volReqId(0) {
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
// usage 
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::usage(std::ostream &os,
  const char *const programName) throw() {
  os <<
    "Usage:\n"
    "\t" << programName << " ACTION VID [OPTIONS]... [FILE]...\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID    The VID of the tape to be copied to/from\n"
    "\tACTION ";
  Action::writeValidStrings(os, " or ");
  os <<
    " (case insensitive)\n"
    "\tFILE   A filename in RFIO notation [host:]local_path\n"
    "\n"
    "Options common to all actions: ";
  Action::writeValidStrings(os, " or ");
  os <<
    "\n"
    "\n"
    "\t-d, --debug             Print debug information\n"
    "\t-h, --help              Print this help and exit\n"
    "\t-s, --server server     Specifies the tape server to be used therefore\n"
    "\t                        overriding the drive scheduling of the VDQM\n"
    "\n"
    "Options that apply to the READ action:\n"
    "\n"
    "\t-f, --filelist          File containing a list of filenames\n"
    "\t-q, --sequence sequence The tape file sequences\n"
    "\n"
    "Options that apply to the WRITE action:\n"
    "\n"
    "\t-f, --filelist          File containing a list of filenames\n"
    "\t-p, --position          Either the string \"EOT\" meaning append to\n"
    "\t                        End Of Tape, or the tape file sequence number\n"
    "\t                        to be positioned to just before writing.  Note\n"
    "\t                        this option is mandatory when the action is\n"
    "\t                        WRITE.  Also not that a valid tape file\n"
    "\t                        sequence number is equal or greater than 1.\n"
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
void castor::tape::tpcp::TpcpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

  static struct option longopts[] = {
    {"debug"   ,       NO_ARGUMENT, NULL, 'd'},
    {"filelist", REQUIRED_ARGUMENT, NULL, 'f'},
    {"help"    ,       NO_ARGUMENT, NULL, 'h'},
    {"sequence", REQUIRED_ARGUMENT, NULL, 'q'},
    {"server"  , REQUIRED_ARGUMENT, NULL, 's'},
    {"position", REQUIRED_ARGUMENT, NULL, 'p'},
    {NULL      , 0                , NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":df:hq:p:", longopts, NULL)) != -1) {

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

    case 'p':
      {
        m_cmdLine.tapeFseqPositionSet = true;

        std::string tmp(optarg);
        utils::toUpper(tmp);

        if(tmp == "EOT") {
          m_cmdLine.tapeFseqPosition = 0;
        } else {

          if(!utils::isValidUInt(optarg)) {
            castor::exception::InvalidArgument ex;
            ex.getMessage() <<
              "\tThe -p, --position argument must either be the string \"EOT\" "
              "or a valid\n\tunsigned integer greater than 0: Actual=\""
              << optarg << "\"";
            throw ex;
          }

          m_cmdLine.tapeFseqPosition = atoi(optarg);

          if(m_cmdLine.tapeFseqPosition == 0) {
            castor::exception::InvalidArgument ex;
            ex.getMessage() <<
              "\tThe -p, --position argument must be a valid unsigned integer "
              "greater\n\tthan 0: Actual=" << optarg;
            throw ex;
          }
        }
      }
      break;

    case 'q':
      TapeFileSequenceParser::parse(optarg, m_cmdLine.tapeFseqRanges);
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

  // Check the minimum number of command-line arguments are present
  if(argc-optind < TPCPMINARGS){
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tWrong number of command-line arguments: Actual=" <<
      argc-optind << " Expected minimum=" << TPCPMINARGS; 

    throw ex;
  }

  const int nbFilenamesOnCommandLine = argc - optind - TPCPMINARGS;

  // Check that filenames as command-line arguments and the "-f, --filelist"
  // command-line option have not been specified at the same time, as they are
  // mutually exclusive
  if(nbFilenamesOnCommandLine > 0 && m_cmdLine.fileListSet) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\t[FILE].. command-line arguments and the"
       " \"-f, --filelist\" option are\n\tmutually exclusive";

    throw ex;
  }

  // Parse the action command-line argument
  try {
    utils::toUpper(argv[optind]);
    m_cmdLine.action = Action::stringToObject((char*)argv[optind]);
  } catch(castor::exception::InvalidArgument) {

    castor::exception::InvalidArgument ex;
    std::ostream &os = ex.getMessage();

    os << "\tFirst command-line argument must be a valid Action:\n\tActual=" <<
      argv[optind] << " Expected=";

    Action::writeValidStrings(os, " or ");

    throw ex;
  }

  // Move on to the next command-line argument
  optind++;

  try {
    utils::checkVidSyntax(argv[optind]);
  } catch(castor::exception::InvalidArgument &ex) {
    castor::exception::InvalidArgument ex2;

    ex2.getMessage() << "\tSecond command-line argument must be a valid VID:\n"
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

  // Move on to the next command-line argument (there may not be one)
  optind++;

  // Parse any filenames on the command-line
  while(optind < argc) {
    m_cmdLine.filenames.push_back(argv[optind++]);
  }

  // If the action to be performed is a recall
  if(m_cmdLine.action == Action::read) {
    // Check that there is at least one file to be recalled
    if(m_cmdLine.tapeFseqRanges.size() == 0) {
      castor::exception::InvalidArgument ex;

      ex.getMessage()
        << "\tThere must be at least one tape file sequence number when "
           "recalling";

      throw ex;
    }
  }

  // Check that there is no more than one tape file sequence range which goes
  // to END of the tape
  const unsigned int nbRangesWithEnd = countNbRangesWithEnd();
  if(nbRangesWithEnd > 1) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "\tThere cannot be more than one tape file sequence range whose upper "
         "boundary is end of tape";

    throw ex;
  }

  // The -p, --position option is mandatory when the action is WRITE
  if(m_cmdLine.action == Action::write &&
    !m_cmdLine.tapeFseqPositionSet) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "\tThe -p, --position option is mandatory when the action is WRITE";

    throw ex;
  }
}



//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::main(const int argc, char **argv) throw() {

  try {
    const uid_t userId  = getuid();
    const gid_t groupId = getgid();
    
    // Exit with an error message if tpcp is being run as root
    if(userId == 0 && groupId == 0) {
      std::cerr << "tpcp cannot be ran as root" << std::endl
                << std::endl;
      return 1;
    }

    // Set the VMGR error buffer so that the VMGR does not write errors to
    // stderr
    vmgr_error_buffer[0] = '\0';
    if (vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
      std::cerr << "Failed to set VMGR error buffer" << std::endl;
      return 1;
    }

    // Parse the command line
    try {
      parseCommandLine(argc, argv);
    } catch (castor::exception::Exception &ex) {
      std::cerr
        << std::endl
        << "Failed to parse the command-line:\n\n"
        << ex.getMessage().str() << std::endl
        << std::endl;
      castor::tape::tpcp::TpcpCommand::usage(std::cerr, TPCPPROGRAMNAME);
      std::cerr << std::endl;
      return 1;
    }

    // If debug, then display parsed command-line arguments
    if(m_cmdLine.debugSet) {
      std::ostream &os = std::cout;

      os << "Parsed command-line = " << m_cmdLine << std::endl;
    }

    // Display usage message and exit if help option found on command-line
    if(m_cmdLine.helpSet) {
      std::cout << std::endl;
      castor::tape::tpcp::TpcpCommand::usage(std::cout, TPCPPROGRAMNAME);
      std::cout << std::endl;
      return 0;
    }

    // Abort if the requested action type is not yet supportd
    if(m_cmdLine.action != Action::read  &&
       m_cmdLine.action != Action::write &&
       m_cmdLine.action != Action::dump) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage()
        << "tpcp currently only supports the READ and WRITE actions";

      throw ex;
    }

    // Fill the list of filenames to be processed by the action handlers.
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

    // If debug, then display the list of files to be processed by the action
    // handlers
    if(m_cmdLine.debugSet) {
      std::ostream &os = std::cout;

      os << "Filenames to be processed = " << m_filenames << std::endl;
    }

    if(m_cmdLine.action == Action::read) {

      // Check that there are enough RFIO filenames to satisfy the minium number
      // of tape file sequence numbers
      const unsigned int minNbFiles = calculateMinNbOfFiles();
      if(m_filenames.size() < minNbFiles) {
        castor::exception::InvalidArgument ex;

        ex.getMessage()
          << "There are not enough RFIO filenames to cover the minimum number "
             "of tape file sequence numbers"
             ": Actual=" << m_filenames.size() << " Expected minimum="
          << minNbFiles;

        throw ex;
      }
    }

    if(m_cmdLine.action == Action::write) {

      // Check that there is at least one file to be migrated
      if(m_filenames.size() == 0) {
        castor::exception::InvalidArgument ex;

        ex.getMessage()
          << "There must be at least one file to be migrated";

        throw ex;
      }
    }

    // Get information about the tape to be used from the VMGR
    try {
      const int side = 0;
      vmgrQueryTape(m_cmdLine.vid, side);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ECANCELED);

      std::ostream &os = ex2.getMessage();
      os << "Failed to query the VMGR about tape: VID="
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
         ": Request VID=" << m_cmdLine.vid <<
         " VID returned from VMGR=" << m_vmgrTapeInfo.vid;

       throw ex;
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

    // Setup the aggregator callback socket
    setupCallbackSock();

    // If debug, then display a textual description of the aggregator callback
    // socket
    if(m_cmdLine.debugSet) {
      std::ostream &os = std::cout;

      os << "Aggregator callback socket details = ";
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
      os << ": Waiting for a drive: Volume request ID = " << m_volReqId
         << std::endl;
    }

    // Socket file descriptor for a callback connection from the aggregator
    int connectionSockFd = 0;

    // Wait for a callback connection from the aggregator
    {
      bool waitForCallback    = true;
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
          os << ": Waited " << WAITCALLBACKTIMEOUT << " seconds for a "
          "callback connection from the tape server." << std::endl
          << "Continuing to wait." <<  std::endl;
        }
      }
    }

    // Command-line user feedback
    {
      std::ostream &os = std::cout;
      time_t       now = time(NULL);

      utils::writeTime(os, now, TIMEFORMAT);
      os << ": Received connection from ";

      unsigned long  peerIp    = 0;
      unsigned short peerPort  = 0;
      try {
        net::getPeerIpPort(connectionSockFd, peerIp, peerPort);
      } catch(castor::exception::Exception &e) {
        peerIp   = 0;
        peerPort = 0;
      }
      net::writeIp(os, peerIp);
      os << ":" << peerPort << std::endl;
    }

    // Wrap the connection socket descriptor in a CASTOR framework socket in
    // order to get access to the framework marshalling and un-marshalling
    // methods
    castor::io::AbstractTCPSocket callbackConnectionSock(connectionSockFd);

    // Read in the object sent by the aggregator
    std::auto_ptr<castor::IObject> obj(callbackConnectionSock.readObject());

    // Pointer to the received object with the object's type
    tapegateway::VolumeRequest *volumeRequest = NULL;

    // Cast the object to its type
    volumeRequest = dynamic_cast<tapegateway::VolumeRequest*>(obj.get());
    if(volumeRequest == NULL) {
      castor::exception::InvalidArgument ex;

      ex.getMessage()
        << "Received the wrong type of object from the aggregator"
        << ": Actual=" << utils::objectTypeToString(obj->type())
        << " Expected=VolumeRequest";

      throw ex;
    }

    Helper::displayRcvdMsgIfDebug(*volumeRequest, m_cmdLine.debugSet);

    {
      std::ostream &os = std::cout;
      time_t       now = time(NULL);

      utils::writeTime(os, now, TIMEFORMAT);
      os << ": Tape mounted on drive " << volumeRequest->unit() << std::endl;
    }

    // Check the volume request ID of the VolumeRequest object matches that of
    // the reply from the VDQM when the drive was requested
    if(volumeRequest->mountTransactionId() != (uint64_t)m_volReqId) {
      castor::exception::InvalidArgument ex;

      ex.getMessage()
        << "Received the wrong mount transaction ID from the aggregator"
        << ": Actual=" << volumeRequest->mountTransactionId()
        << " Expected=" <<  m_volReqId;

      throw ex;
    }

    // Create the volume message for the aggregator
    castor::tape::tapegateway::Volume volumeMsg;
    volumeMsg.setVid(m_vmgrTapeInfo.vid);
    switch(m_cmdLine.action.value()) {
    case Action::READ:
      volumeMsg.setMode(castor::tape::tapegateway::READ);
      break;
    case Action::WRITE:
      volumeMsg.setMode(castor::tape::tapegateway::WRITE);
      break;
    case Action::DUMP:
      volumeMsg.setMode(castor::tape::tapegateway::DUMP);
      break;
    default:
      TAPE_THROW_EX(castor::exception::Internal,
        ": Unknown action type: value=" << m_cmdLine.action.value());
    }
    volumeMsg.setMode(m_cmdLine.action == Action::write ?
      castor::tape::tapegateway::WRITE : castor::tape::tapegateway::READ);
    volumeMsg.setLabel(m_vmgrTapeInfo.lbltype);
    volumeMsg.setMountTransactionId(m_volReqId);
    volumeMsg.setDensity(m_vmgrTapeInfo.density);

    // Send the volume message to the aggregator
    callbackConnectionSock.sendObject(volumeMsg);

    Helper::displaySentMsgIfDebug(volumeMsg, m_cmdLine.debugSet);

    // Close the connection to the aggregator
    callbackConnectionSock.close();

    // Dispatch the action to the appropriate ActionHandler
    try {
      dispatchAction();
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ECANCELED);

      ex2.getMessage() << "Failed to perform action"
        ": Action=" << m_cmdLine.action <<
        ": " << ex.getMessage().str();

      throw ex2;
    }
  } catch(castor::exception::Exception &ex) {
    std::cerr << std::endl
      << "Aborting: "
      << ex.getMessage().str()
      << std::endl
      << std::endl;
    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// getVdqmListenPort()
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::getVdqmListenPort()
  throw(castor::exception::Exception) {

  int port = AGGREGATOR_VDQMPORT; // Initialise to default value

  const char *const configEntry = getconfent("TAPEAGGREGATOR", "VDQMPORT", 0);

  if(configEntry != NULL) {
    if(utils::isValidUInt(configEntry)) {
      port = atoi(configEntry);
    } else {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Invalid configuration entry:" 
      << configEntry);
    }
  }

  return port;
}


//------------------------------------------------------------------------------
// calculateMinNbOfFiles
//------------------------------------------------------------------------------
unsigned int castor::tape::tpcp::TpcpCommand::calculateMinNbOfFiles()
  throw (castor::exception::Exception) {
  
  unsigned int count = 0;

  // Loop through all ranges
  for(TapeFseqRangeList::const_iterator itor=
    m_cmdLine.tapeFseqRanges.begin();
    itor!=m_cmdLine.tapeFseqRanges.end(); itor++) {
  
    // If upper != END of tape
    if(itor->upper != 0 ){
        count += (itor->upper - itor->lower) + 1;
    } else {
      count++;
    }
  }
  return count; 
}


//------------------------------------------------------------------------------
// countNbRangesWithEnd
//------------------------------------------------------------------------------
unsigned int castor::tape::tpcp::TpcpCommand::countNbRangesWithEnd()
  throw (castor::exception::Exception) {

  unsigned int count = 0;

  // Loop through all ranges
  for(TapeFseqRangeList::const_iterator itor=
    m_cmdLine.tapeFseqRanges.begin();
    itor!=m_cmdLine.tapeFseqRanges.end(); itor++) {

    if(itor->upper == 0 ){
        count++;
    } 
  }
  return count;
}


//------------------------------------------------------------------------------
// vmgrQueryTape
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::vmgrQueryTape(
  char (&vid)[CA_MAXVIDLEN+1], const int side)
  throw (castor::exception::Exception) {

  int save_serrno = 0;

  serrno=0;
  const int rc = vmgr_querytape(m_cmdLine.vid, side, &m_vmgrTapeInfo,
    m_dgn);
  
  save_serrno = serrno;

  if(rc != 0) {
    char buf[STRERRORBUFLEN];
    sstrerror_r(serrno, buf, sizeof(buf));
    buf[sizeof(buf)-1] = '\0';
    TAPE_THROW_CODE(save_serrno,
      ": Failed vmgr_querytape() call"
      ": " << buf);
  }
}

//------------------------------------------------------------------------------
// setupCallbackSock
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::setupCallbackSock()
  throw(castor::exception::Exception) {

  // Get the port range to be used by the aggregator callback socket
  int   lowPort  = LOW_CLIENT_PORT_RANGE;
  int   highPort = HIGH_CLIENT_PORT_RANGE;
  char* sport    = NULL;
  if((sport = getconfent((char *)CLIENT_CONF,(char *)LOWPORT_CONF,0)) != 0) {
    lowPort = castor::System::porttoi(sport);
  }
  if((sport = getconfent((char *)CLIENT_CONF,(char *)HIGHPORT_CONF,0)) != 0) {
    highPort = castor::System::porttoi(sport);
  }

  // Bind the aggregator callback socket
  m_callbackSock.bind(lowPort, highPort);
  m_callbackSock.listen();
}


//------------------------------------------------------------------------------
// requestDriveFromVdqm 
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::requestDriveFromVdqm(const int mode,
  char *const server) throw(castor::exception::Exception) {

  unsigned short port = 0;
  unsigned long  ip   = 0;
  m_callbackSock.getPortIp(port, ip);

  vdqmnw_t *const nw   = NULL;
  char     *const unit = NULL;
  const int rc = vdqm_SendAggregatorVolReq(nw, &m_volReqId,
    m_cmdLine.vid, m_dgn, server, unit, mode, port);
  const int save_serrno = serrno;

  if(rc == -1) {
    char buf[STRERRORBUFLEN];
    sstrerror_r(save_serrno, buf, sizeof(buf));
    buf[sizeof(buf)-1] = '\0';

    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() << "Failed to request drive from VDQM: "
      << buf;

    throw ex;
  }
}


//------------------------------------------------------------------------------
// dispatchAction
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::dispatchAction()
  throw(castor::exception::Exception) {

  switch(m_cmdLine.action.value()) {
  case Action::READ:
    {
      Recaller handler(m_cmdLine, m_filenames, m_vmgrTapeInfo, m_dgn,
        m_volReqId, m_callbackSock);

      handler.run();
    }
    break;
  case Action::WRITE:
    {
      Migrator handler(m_cmdLine, m_filenames, m_vmgrTapeInfo, m_dgn,
        m_volReqId, m_callbackSock);

      handler.run();
    }
    break;
  case Action::DUMP:
    {
      Dumper handler(m_cmdLine, m_filenames, m_vmgrTapeInfo, m_dgn,
        m_volReqId, m_callbackSock);

      handler.run();
    }
    break;
  case Action::VERIFY:
    {
      Verifier handler(m_cmdLine, m_filenames, m_vmgrTapeInfo, m_dgn,
        m_volReqId, m_callbackSock);

      handler.run();
    }
    break;
  default:
    {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to find ActionHandler for Action:  Action="
        << m_cmdLine.action);
    }
    break;
  } // switch(m_cmdLine.action.value())
}
