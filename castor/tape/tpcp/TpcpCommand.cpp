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
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tpcp/Constants.hpp"
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
  m_callbackSocket(false),
  m_volReqId(0) {
  utils::setBytes(m_dgn, '\0');

  // Build the map of ActionHandlers
  m_handlers[Action::READ]   = &m_dataMover;
  m_handlers[Action::WRITE]  = &m_dataMover;
  m_handlers[Action::DUMP]   = &m_dumper;
  m_handlers[Action::VERIFY] = &m_verifier;
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
    "Options:\n"
    "\n"
    "\t-d, --debug             Print debug information\n"
    "\t-f, --filelist          File containing a list of filenames\n"
    "\t-h, --help              Print this help and exit\n"
    "\t-q, --sequence sequence The tape file sequences\n"
    "\n"
    "Constraints:\n"
    "\tThe [FILE].. command-line arguments and the \"-f, --filelist\" option"
    " are mutually exclusive\n"
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
    {NULL      , 0                , NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":df:hq:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      m_parsedCommandLine.debugOptionSet = true;
      break;

    case 'f':
      m_parsedCommandLine.fileListOptionSet = true;
      m_parsedCommandLine.fileListFilename  = optarg;
      break;

    case 'h':
      m_parsedCommandLine.helpOptionSet = true;
      break;

    case 'q':
      parseTapeFileSequence(optarg);
      break;

    case ':':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "The -" << (char)optopt
          << " option requires a parameter";
        throw ex;
      }
      break;

    case '?':
      {
        castor::exception::InvalidArgument ex;

        if(optopt == 0) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
      break;
    default:
      {
        castor::exception::Internal ex;
        ex.getMessage()
          << "getopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;
        throw ex;
      }
    } // switch (c)
  } // while ((c = getopt_long(argc, argv, "h", longopts, NULL)) != -1)

  // There is no need to continue parsing when the help option is set
  if( m_parsedCommandLine.helpOptionSet) {
    return;
  }

  // Check the minimum number of command-line arguments are present
  if(argc-optind < TPCPMINARGS){
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "Wrong number of command-line arguments: Actual=" <<
      argc-optind << " Expected minimum=" << TPCPMINARGS; 

    throw ex;
  }

  const int nbFilenamesOnCommandLine = argc - optind - TPCPMINARGS;

  // Check that filenames as command-line arguments and the "-f, --filelist"
  // command-line option have not been specified at the same time, as they are
  // mutually exclusive
  if(nbFilenamesOnCommandLine > 0 && m_parsedCommandLine.fileListOptionSet) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "[FILE].. command-line arguments and the"
       " \"-f, --filelist\" option are mutually exclusive";

    throw ex;
  }

  // Parse the action command-line argument
  try {
    utils::toUpper(argv[optind]);
    m_parsedCommandLine.action = Action::stringToObject((char*)argv[optind]);
  } catch(castor::exception::InvalidArgument) {

    castor::exception::InvalidArgument ex;
    std::ostream &os = ex.getMessage();

    os << "First command-line argument must be a valid Action: Actual=" <<
      argv[optind] << " Expected=";

    Action::writeValidStrings(os, " or ");

    throw ex;
  }

  // Move on to the next command-line argument
  optind++;

  // Parse the VID command-line argument
  try {
    utils::copyString(m_parsedCommandLine.vid, argv[optind]);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to copy VID comand-line argument into internal data structures"
      ": " << ex.getMessage().str());
  }
  try {
    utils::checkVidSyntax(m_parsedCommandLine.vid);
  } catch(castor::exception::InvalidArgument &ex) {
    castor::exception::InvalidArgument ex2;

    ex2.getMessage() << "Second command-line argument must be a valid VID: " <<
      ex.getMessage().str();

    throw ex2;
  }

  // Move on to the next command-line argument (there may not be one)
  optind++;

  // Parse any filenames on the command-line
  while(optind < argc) {
    m_parsedCommandLine.filenames.push_back(argv[optind++]);
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
        << "Failed to parse the command-line:\n\n\t"
        << ex.getMessage().str() << std::endl;
      castor::tape::tpcp::TpcpCommand::usage(std::cerr, TPCPPROGRAMNAME);
      std::cerr << std::endl;
      return 1;
    }

    // If debug, then display parsed command-line arguments
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeParsedCommandLine(os);
      os << std::endl;
    }

    // Display usage message and exit if help option found on command-line
    if(m_parsedCommandLine.helpOptionSet) {
      std::cout << std::endl;
      castor::tape::tpcp::TpcpCommand::usage(std::cout, TPCPPROGRAMNAME);
      std::cout << std::endl;
      return 0;
    }

    // Fill the list of filenames to be processed by the action handlers.
    // The list of filenames will either come from the command-line arguments
    // or (exclusive or) from a "filelist" file specified with the
    // "-f, --filelist" option.
    if(m_parsedCommandLine.fileListOptionSet) {
      // Parse the "filelist" file into the list of filenames to be
      // processed
      utils::parseFileList(m_parsedCommandLine.fileListFilename.c_str(),
        m_filenames);
    } else {
      // Copy the command-line argument filenames into the list of filenames
      // to be processed
      for(FilenameList::const_iterator
        itor=m_parsedCommandLine.filenames.begin();
        itor!=m_parsedCommandLine.filenames.end(); itor++) {
        m_filenames.push_back(*itor);
      }
    }

    // If debug, then display the list of files to be processed by the action
    // handlers
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeFilenamesToBeProcessed(os);
      os << std::endl;
    }

    // Get the DGN of the tape to be used
    vmgr_tape_info tapeInfo;

    try {
      utils::setBytes(tapeInfo, '\0');
      const int side = 0;
      vmgrQueryTape(m_parsedCommandLine.vid, side, tapeInfo, m_dgn);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ECANCELED);

      std::ostream &os = ex2.getMessage();
      os << "Failed to query the VMGR about tape: VID="
         << m_parsedCommandLine.vid;

      // If the tape does not exist
      if(ex.code() == ENOENT) {
        os << ": Tape does not exist";
      } else {
        os << ": " << ex.getMessage().str();
      }

      throw ex2;
    }

    // If debug, then display the tape information retrieved from the VMGR
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeVmgrTapeInfo(os, tapeInfo);
      os << std::endl;
      writeDgn(os);
      os << std::endl;
    }

    // Check the tape is available
    if(tapeInfo.status & DISABLED ||
       tapeInfo.status & EXPORTED ||
       tapeInfo.status & ARCHIVED) {

       castor::exception::Exception ex(ECANCELED);
       std::ostream &os = ex.getMessage();

       os << "Tape is not available: Tape is: ";

       if(tapeInfo.status & DISABLED) os << " DISABLED";
       if(tapeInfo.status & EXPORTED) os << " EXPORTED";
       if(tapeInfo.status & ARCHIVED) os << " ARCHIVED";

       throw ex;
    }

    // Check if the access mode of the tape is compatible with the action to be
    // performed by tpcp
    if(m_parsedCommandLine.action == Action::write &&
      tapeInfo.status & TAPE_RDONLY) {

      castor::exception::Exception ex(ECANCELED);

       ex.getMessage() << "Tape cannot be written to"
         ": Tape marked as TAPE_RDONLY";

       throw ex;
    }

    // Setup the aggregator callback socket
    setupCallbackSocket();

    // If debug, then display a textual description of the aggregator callback
    // socket
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeCallbackSocket(os);
      os << std::endl;
    }

    // Send the request for a drive to the VDQM
    {
      const int mode = m_parsedCommandLine.action == Action::write ?
        WRITE_ENABLE : WRITE_DISABLE;
      requestDriveFromVdqm(mode);
    }

    // If debug, then display the volume request ID returned by the VDQM
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeVolReqId(os);
      os << std::endl;
    }

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
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      writeAggregatorCallbackConnection(os, connectionSocketFd);
      os << std::endl;
    }

    // Wrap the connection socket descriptor in CASTOR framework socket in
    // order to get access to the framework marshalling and un-marshalling
    // methods
    castor::io::AbstractTCPSocket callbackConnectionSocket(connectionSocketFd);

    // Read in the first object sent by the aggregator
    std::auto_ptr<castor::IObject> obj(callbackConnectionSocket.readObject());

    // Pointer to the received object with the object's type
    tapegateway::VolumeRequest *volumeRequest = NULL;

    // Cast the object to its type, i.e. VolumeRequest
    volumeRequest = dynamic_cast<tapegateway::VolumeRequest*>(obj.get());
    if(volumeRequest == NULL) {
      castor::exception::InvalidArgument ex;

      ex.getMessage()
        << "Received the wrong type of object from the aggregator"
        << ": Actual=" << utils::objectTypeToString(obj->type())
        << " Expected=OBJ_VolumeRequest";

      throw ex;
    }

    // Check the volume request ID of the VolumeRequest object matches that of
    // the reply from the VDQM when the drive was requested
    if(volumeRequest->vdqmVolReqId() != m_volReqId) {
      castor::exception::InvalidArgument ex;

      ex.getMessage()
        << "Received the wrong volume request ID from the aggregator"
        << ": Actual=" << volumeRequest->vdqmVolReqId()
        << " Expected=" <<  m_volReqId;

      throw ex;
    }

    // Find the appropriate ActionHandler
    ActionHandler *handler = NULL;
    {
      ActionHandlerMap::iterator itor = m_handlers.find(
        m_parsedCommandLine.action.value());
      if(itor == m_handlers.end()) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to find ActionHandler: Action="
          << m_parsedCommandLine.action);
      }
      handler = itor->second;
    }

    // Run the action handler
    try {
      handler->run(m_parsedCommandLine.debugOptionSet,
        m_parsedCommandLine.action, m_parsedCommandLine.vid,
        m_parsedCommandLine.tapeFseqRanges, m_filenames, m_dgn, m_volReqId,
        m_callbackSocket);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex2(ECANCELED);

      ex2.getMessage() << "Failed to perform action"
        ": Action=" << m_parsedCommandLine.action <<
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
// writeParsedCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeParsedCommandLine(std::ostream &os)
  throw() {
  os << "===================" << std::endl
     << "Parsed command-line" << std::endl
     << "===================" << std::endl
     << std::endl
     << m_parsedCommandLine;
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
    } 
    else {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Invalid configuration entry:" 
      << configEntry);
      }
  }

  return port;
}


//------------------------------------------------------------------------------
// parseTapeFileSequence
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::parseTapeFileSequence(
  char *const str) throw (castor::exception::Exception) {

  std::vector<std::string> rangeStrs;
  int nbBoundaries = 0;
  TapeFseqRange range;

  // Range strings are separated by commas
  utils::splitString(str, ',', rangeStrs);

  // For each range string
  for(std::vector<std::string>::const_iterator itor=rangeStrs.begin();
    itor!=rangeStrs.end(); itor++) {

    std::vector<std::string> boundaryStrs;

    // Lower and upper boundary strings are separated by a dash ('-')
    utils::splitString(*itor, '-', boundaryStrs);

    nbBoundaries = boundaryStrs.size();

    switch(nbBoundaries) {
    case 1: // Range string = "n"
      if(!utils::isValidUInt(boundaryStrs[0].c_str())) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << boundaryStrs[0]
          << "': Expecting an unsigned integer";
        throw ex;
      }

      range.lower = range.upper = atoi(boundaryStrs[0].c_str());
      m_parsedCommandLine.tapeFseqRanges.push_back(range);
      break;

    case 2: // Range string = "m-n" or "-n" or "m-" or "-"

      // If "-n" or "-" then the range string is invalid
      if(boundaryStrs[0] == "") {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
          << "': Strings of the form '-n' or '-' are invalid";
        throw ex;
      }

      // At this point the range string must be either "m-n" or "m-"

      // Parse the "m" of "m-n" or "m-"
      if(!utils::isValidUInt(boundaryStrs[0].c_str())) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
          << "': The lower boundary should be an unsigned integer";
        throw ex;
      }

      range.lower = atoi(boundaryStrs[0].c_str());
      if(range.lower == 0){
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
          << "': The lower boubary can not be '0'";
        throw ex;
      }
      // If "m-"
      if(boundaryStrs[1] == "") {
        // Inifinity (or until the end of tape) is represented by 0
        range.upper = 0;
      // Else "m-n"
      } else {
        // Parse the "n" of "m-n"
        if(!utils::isValidUInt(boundaryStrs[1].c_str())) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor
            << "': The upper boundary should be an unsigned integer";
          throw ex;
        }
        range.upper = atoi(boundaryStrs[1].c_str());

        if(range.upper == 0){
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor
            << "': The uppre boubary can not be '0'";
          throw ex;
        }
        if(range.lower > range.upper){
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor
            << "': The lower boundary cannot be greater than the upper "
            "boundary";
          throw ex;
        }
      }

      m_parsedCommandLine.tapeFseqRanges.push_back(range);

      break;

    default: // Range string is invalid
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "Invalid range string: '" << *itor
        << "': A range string can only contain one or no dashes ('-')";
      throw ex;
    }
  }
}


//------------------------------------------------------------------------------
// writeFilenamesToBeProcessed
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeFilenamesToBeProcessed(
  std::ostream &os) throw() {

  os << "=========================" << std::endl
     << "Filenames to be processed" << std::endl
     << "=========================" << std::endl
     << std::endl
     << "filenames = " << m_filenames;
}


//------------------------------------------------------------------------------
// countMinNumberOfFiles
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::countMinNumberOfFiles()
  throw (castor::exception::Exception) {
  
  int count = 0;
  // Loop through all ranges
  for(TapeFseqRangeList::const_iterator itor=
    m_parsedCommandLine.tapeFseqRanges.begin();
    itor!=m_parsedCommandLine.tapeFseqRanges.end(); itor++) {
  
    if(itor->upper != 0 ){
        count += (itor->upper - itor->lower) + 1;
    } else {
      count++;
    }
  }
  return count; 
}


//------------------------------------------------------------------------------
// nbRangesWithEnd
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::nbRangesWithEnd()
  throw (castor::exception::Exception) {

  int count = 0;
  // Loop through all ranges
  for(TapeFseqRangeList::const_iterator itor=
    m_parsedCommandLine.tapeFseqRanges.begin();
    itor!=m_parsedCommandLine.tapeFseqRanges.end(); itor++) {

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
  char (&vid)[CA_MAXVIDLEN+1], const int side, vmgr_tape_info &tapeInfo,
  char (&dgn)[CA_MAXVIDLEN+1]) throw (castor::exception::Exception) {

  int save_serrno = 0;

  serrno=0;
  const int rc = vmgr_querytape(m_parsedCommandLine.vid, side, &tapeInfo, dgn);
  
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
// writeVmgrTapeInfo
//------------------------------------------------------------------------------
void  castor::tape::tpcp::TpcpCommand::writeVmgrTapeInfo(std::ostream &os,
  vmgr_tape_info &info) throw() {
  os << "============================" << std::endl
     << "vmgr_tape_info from the VMGR" << std::endl
     << "============================" << std::endl
     << std::endl
     << "vid                  = \"" << info.vid << "\""          << std::endl
     << "vsn                  = \"" << info.vsn << "\""          << std::endl
     << "library              = \"" << info.library << "\""      << std::endl
     << "density              = \"" << info.density << "\""      << std::endl
     << "lbltype              = \"" << info.lbltype << "\""      << std::endl
     << "model                = \"" << info.model << "\""        << std::endl
     << "media_letter         = \"" << info.media_letter << "\"" << std::endl
     << "manufacturer         = \"" << info.manufacturer << "\"" << std::endl
     << "sn                   = \"" << info.sn << "\""           << std::endl
     << "nbsides              = " << info.nbsides                << std::endl
     << "etime                = " << info.etime                  << std::endl
     << "rcount               = " << info.rcount                 << std::endl
     << "wcount               = " << info.wcount                 << std::endl
     << "rhost                = \"" << info.rhost << "\""        << std::endl
     << "whost                = \"" << info.whost << "\""        << std::endl
     << "rjid                 = " << info.rjid                   << std::endl
     << "wjid                 = " << info.wjid                   << std::endl
     << "rtime                = " << info.rtime                  << std::endl
     << "wtime                = " << info.wtime                  << std::endl
     << "side                 = " << info.side                   << std::endl
     << "poolname             = \"" << info.poolname << "\""     << std::endl
     << "status               = " << info.status                 << std::endl
     << "estimated_free_space = " << info.estimated_free_space   << std::endl
     << "nbfiles              = " << info.nbfiles                << std::endl;
}


//------------------------------------------------------------------------------
// writeDgn
//------------------------------------------------------------------------------
void  castor::tape::tpcp::TpcpCommand::writeDgn(std::ostream &os) throw() {
  os << "=================" << std::endl
     << "DGN from the VMGR" << std::endl
     << "=================" << std::endl
     << std::endl
     << "DGN=\"" << m_dgn << "\"" << std::endl;
}


//------------------------------------------------------------------------------
// setupCallbackSocket
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::setupCallbackSocket()
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
  m_callbackSocket.bind(lowPort, highPort);
  m_callbackSocket.listen();
}


//------------------------------------------------------------------------------
// writeCallbackSocket
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeCallbackSocket(std::ostream &os)
  throw() {
  os << "==================================" << std::endl
     << "Aggregator callback socket details" << std::endl
     << "==================================" << std::endl
     << std::endl;

  net::writeSocketDescription(os, m_callbackSocket.socket());

  os << std::endl;
}


//------------------------------------------------------------------------------
// requestDriveFromVdqm 
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::requestDriveFromVdqm(const int mode)
  throw(castor::exception::Exception) {

  unsigned short port = 0;
  unsigned long  ip   = 0;
  m_callbackSocket.getPortIp(port, ip);

  vdqmnw_t *const nw     = NULL;
  char     *const server = NULL;
  char     *const unit   = NULL;
  const int rc = vdqm_SendAggregatorVolReq(nw, &m_volReqId,
    m_parsedCommandLine.vid, m_dgn, server, unit, mode, port);
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
// writeVolReqId
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeVolReqId(std::ostream &os) throw() {
  os << "======================================" << std::endl
     << "Volume request ID returned by the VDQM" << std::endl
     << "======================================" << std::endl
     << std::endl
     << "volReqId=" << m_volReqId << std::endl;
}


//------------------------------------------------------------------------------
// writeAggregatorCallbackConnection
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeAggregatorCallbackConnection(
  std::ostream &os, const int connectSocketFd) throw() {
  os << "==============================" << std::endl
     << "Aggregator callback connection" << std::endl
     << "==============================" << std::endl
     << std::endl;
  net::writeSocketDescription(os, connectSocketFd);
  os << std::endl;
}
