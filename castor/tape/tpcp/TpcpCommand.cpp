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
 
#include "castor/PortNumbers.hpp"
#include "castor/System.hpp"
#include "castor/exception/Internal.hpp" 
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
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
#include <poll.h>


//------------------------------------------------------------------------------
// vmgr_error_buffer
//------------------------------------------------------------------------------
char castor::tape::tpcp::TpcpCommand::vmgr_error_buffer[VMGRERRORBUFLEN];


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::TpcpCommand() throw () :
  m_callbackSocket(false) {
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
    "\tFILE   A file path in RFIO notation [host:]local_path\n"
    "\n"
    "Options:\n"
    "\n"
    "\t-d, --debug             Print debug information\n"
    "\t-f, --file              List of file name\n"
    "\t-h, --help              Print this help and exit\n"
    "\t-q, --sequence sequence The tape file sequences\n"
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
    {"file"   ,        NO_ARGUMENT, NULL, 'f'},
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
      parseFilenames(optarg);
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

  // Check the number of command-line arguments
  if(argc-optind != TPCP_NB_ARGS){
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "Wrong number of command-line arguments: Actual=" <<
      argc-optind << " Expected=" << TPCP_NB_ARGS; 

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
}



//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::main(const int argc, char **argv) throw() {

  try {
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

    // Get the DGN of the tape to be used
    vmgr_tape_info tapeInfo;

    try {
      utils::setBytes(tapeInfo, '\0');
      const int side = 0;//1;
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

    // Setup the aggregator callback socket
    // TO BE DONE

    // Send the request for a drive to the VDQM
    // TO BE DONE

    // Wait for the callback from the aggregator
    // TO BE DONE

    // Dispatch the Action to appropriate Action handler
    try {
      switch(m_parsedCommandLine.action.value()) {
      case Action::READ:
        m_dataMover.run(m_parsedCommandLine, m_dgn, m_callbackSocket);
        break;
      case Action::WRITE:
        m_dataMover.run(m_parsedCommandLine, m_dgn, m_callbackSocket);
        break;
      case Action::DUMP:
        m_dumper.run(m_parsedCommandLine, m_dgn, m_callbackSocket);
        break;
      case Action::VERIFY:
        m_verifier.run(m_parsedCommandLine, m_dgn, m_callbackSocket);
        break;
      default:
        {
          castor::exception::Internal ex;

          ex.getMessage() << "Action unknown to ActionHandler dispatch logic";
          throw(ex);
        }
      }
    } catch(castor::exception::Exception &ex) {
      std::cerr << "Failed to perform " << m_parsedCommandLine.action.str()
        << " action: " << ex.getMessage().str();

      return 1;
    }
  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl
      << "Aborting: Unexpected exception: "
      << ex.getMessage().str()
      << std::endl
      << std::endl;
    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// writeTapeFseqRangeList
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeTapeFseqRangeList(std::ostream &os,
  castor::tape::tpcp::Uint32RangeList &list) {
  for(Uint32RangeList::iterator itor=list.begin(); itor!=list.end(); itor++) {
    if(itor!=list.begin()) {
      os << ",";
    }

    os << itor->lower << "-";

    // Write out the  upper bound, where 0 means end of tape
    if(itor->upper > 0) {
      os << itor->upper;
    } else {
      os << "END";
    }
  }
}

//------------------------------------------------------------------------------
// writeFilenameList
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeFilenameList(std::ostream &os,
  std::list<std::string> &list) {
  for(std::list<std::string>::iterator itor=list.begin(); itor!=list.end();
    itor++) {
    if(itor!=list.begin()) {
      os << ",";
    }

    os << *itor; 
  }
}


//------------------------------------------------------------------------------
// writeParsedCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeParsedCommandLine(std::ostream &os) {
  os << "===================" << std::endl
     << "Parsed command-line" << std::endl
     << "===================" << std::endl
     << std::endl
     << "debugOptionSet ="
     << utils::boolToString(m_parsedCommandLine.debugOptionSet)
     << std::endl
     << "helpOptionSet  ="
     << utils::boolToString(m_parsedCommandLine.helpOptionSet)
     << std::endl
     << "action         ="
     << m_parsedCommandLine.action.str()
     << std::endl
     << "vid            =";
  if(m_parsedCommandLine.vid == NULL) {
    os << "NULL";
  } else {
    os << "\"" << m_parsedCommandLine.vid << "\"";
  }
  os << std::endl
     << "tapeFseqRanges =";
  writeTapeFseqRangeList(os, m_parsedCommandLine.tapeFseqRanges);
  os << std::endl
     << "filenamesList  =";
  writeFilenameList(os, m_parsedCommandLine.filenamesList);
  os << std::endl
     << "NumOfFiles     ="<< countMinNumberOfFiles()
     << std::endl
     << "nbRangesWithEnd="<< nbRangesWithEnd()
     << std::endl;
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
// waitForCallBack
//------------------------------------------------------------------------------
castor::io::ServerSocket* castor::tape::tpcp::TpcpCommand::waitForCallBack()
  throw (castor::exception::Exception) {
  
  int rc = 0; 
  struct pollfd pollit;

  pollit.fd      = m_callbackSocket.socket();
  pollit.events  = POLLIN;
  pollit.revents = 0;
  
  // Wait for an incoming connection
  while (1) {
    rc = poll(&pollit, 1, RTCPDNETRWTIMEOUT);
    if (rc == 0) {
     // Log TIMEOUT
    } else 
      if (rc < 0) {
        if (errno == EINTR) {
          continue; // A signal was caught during poll()
        }
        TAPE_THROW_EX(castor::exception::Internal,
          ": Poll erro. Fd= "
          << pollit.fd);
      } else {
        break; // Success
      }
  }

  // Accept the incoming connection
  castor::io::ServerSocket* socket = m_callbackSocket.accept();

  // Log the ip address of the incoming connection
  unsigned short port;
  unsigned long  ip;
  socket->getPeerIp(port, ip);

  std::ostringstream ipToStr;
  ipToStr << ((ip & 0xFF000000) >> 24) << "." << ((ip & 0x00FF0000) >> 16) << "."
  << ((ip & 0x0000FF00) >> 8) << "." << ((ip & 0x000000FF));

  std::cerr<<"Received a connection from -> port: "<<port<<" ip: "<< ipToStr.str().c_str() <<std::endl;


  return socket;
}


//------------------------------------------------------------------------------
// parseTapeFileSequence
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::parseTapeFileSequence(
  char *const str) throw (castor::exception::Exception) {

  std::vector<std::string> rangeStrs;
  int nbBoundaries = 0;
  Uint32Range range;

  // Range strings are separated by commas
  utils::splitString(str, ',', rangeStrs);

  // For each range string
  for(std::vector<std::string>::iterator itor=rangeStrs.begin();
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
// parseFilenames
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::parseFilenames(char *const str)
  throw (castor::exception::Exception) {

  std::vector<std::string> filenameStrs;

  // Filename strings are separated by commas
  utils::splitString(str, ',', filenameStrs);

  // For each filename string
  for(std::vector<std::string>::iterator itor=filenameStrs.begin();
    itor!=filenameStrs.end(); itor++) {

    if((*itor).length() == 0){
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "Invalid file name: '" << *itor
        << "': A filename cannot be an empty string.";
      throw ex;
    }
    m_parsedCommandLine.filenamesList.push_back(*itor);
  }
}


//------------------------------------------------------------------------------
// countMinNumberOfFiles
//------------------------------------------------------------------------------
int castor::tape::tpcp::TpcpCommand::countMinNumberOfFiles()
  throw (castor::exception::Exception) {
  
  int count = 0;
  // Loop through all ranges
  for(Uint32RangeList::iterator itor=m_parsedCommandLine.tapeFseqRanges.begin();
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
  for(Uint32RangeList::iterator itor=m_parsedCommandLine.tapeFseqRanges.begin();
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
  vmgr_tape_info &tapeInfo) {
  os << "============================" << std::endl
     << "vmgr_tape_info from the VMGR" << std::endl
     << "============================" << std::endl
     << std::endl
     << "vid                 =\"" << tapeInfo.vid << "\""          << std::endl
     << "vsn                 =\"" << tapeInfo.vsn << "\""          << std::endl
     << "library             =\"" << tapeInfo.library << "\""      << std::endl
     << "density             =\"" << tapeInfo.density << "\""      << std::endl
     << "lbltype             =\"" << tapeInfo.lbltype << "\""      << std::endl
     << "model               =\"" << tapeInfo.model << "\""        << std::endl
     << "media_letter        =\"" << tapeInfo.media_letter << "\"" << std::endl
     << "manufacturer        =\"" << tapeInfo.manufacturer << "\"" << std::endl
     << "sn                  =\"" << tapeInfo.sn << "\""           << std::endl
     << "nbsides             =" << tapeInfo.nbsides                << std::endl
     << "etime               =" << tapeInfo.etime                  << std::endl
     << "rcount              =" << tapeInfo.rcount                 << std::endl
     << "wcount              =" << tapeInfo.wcount                 << std::endl
     << "rhost               =\"" << tapeInfo.rhost << "\""        << std::endl
     << "whost               =\"" << tapeInfo.whost << "\""        << std::endl
     << "rjid                =" << tapeInfo.rjid                   << std::endl
     << "wjid                =" << tapeInfo.wjid                   << std::endl
     << "rtime               =" << tapeInfo.rtime                  << std::endl
     << "wtime               =" << tapeInfo.wtime                  << std::endl
     << "side                =" << tapeInfo.side                   << std::endl
     << "poolname            =\"" << tapeInfo.poolname << "\""     << std::endl
     << "status              =" << tapeInfo.status                 << std::endl
     << "estimated_free_space=" << tapeInfo.estimated_free_space   << std::endl
     << "nbfiles             =" << tapeInfo.nbfiles                << std::endl;
}


//------------------------------------------------------------------------------
// writeDgn
//------------------------------------------------------------------------------
void  castor::tape::tpcp::TpcpCommand::writeDgn(std::ostream &os) {
  os << "=================" << std::endl
     << "DGN from the VMGR" << std::endl
     << "=================" << std::endl
     << std::endl
     << "DGN=\"" << m_dgn << "\"" << std::endl;
}

/*
  if ( rc == 0 ) {
    if ( ((vmgrTapeInfo.status & DISABLED) == DISABLED) ||
         ((vmgrTapeInfo.status & EXPORTED) == EXPORTED) ||
         ((vmgrTapeInfo.status & ARCHIVED) == ARCHIVED) ) {
      if ( (vmgrTapeInfo.status & DISABLED) == DISABLED ) {
        serrno = ETHELD;
      } else if ( (vmgrTapeInfo.status & EXPORTED) == EXPORTED ) {
        serrno = ETABSENT;
      } else if ( (vmgrTapeInfo.status & ARCHIVED) == ARCHIVED ) {
        serrno = ETARCH;
      }

      castor::exception::Exception ex(serrno);
      ex.getMessage()
        << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
        << " vmgr_querytape failed";
      throw ex;
    }
    return vmgrTapeInfo.status; // break and return the status 

  } else {
    // go on looping
    if ( (save_serrno == SECOMERR) || (save_serrno == EVMGRNACT) ) {
      sleep(5);
    } else {
      castor::exception::Exception ex(save_serrno);
      ex.getMessage()
        << "castor::tape::tapegateway::VmgrTapeGatewayHelper::getDgnFromVmgr"
        << " vmgr_querytape failed";
      throw ex;
    }
  }
*/
