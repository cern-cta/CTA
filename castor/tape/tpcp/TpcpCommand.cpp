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
#include "h/vdqm_api.h"

#include <ctype.h>
#include <getopt.h>
#include <iostream>
#include <list>
#include <poll.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::TpcpCommand::TpcpCommand() throw () :
  m_callbackSocket(false) {
  // Do nothing
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
    {"help"    ,       NO_ARGUMENT, NULL, 'h'},
    {"sequence", REQUIRED_ARGUMENT, NULL, 'q'},
    {NULL      , 0                , NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":dhq:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      m_parsedCommandLine.debugOptionSet = true;
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
  m_parsedCommandLine.vid = argv[optind];
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
int castor::tape::tpcp::TpcpCommand::main(const int argc, char **argv) {

  try {
    // Parse the command line
    try {
      parseCommandLine(argc, argv);
    } catch (castor::exception::Exception &ex) {
      std::cerr << std::endl << "Failed to parse the command-line:\n\n\t"
        << ex.getMessage().str() << std::endl;
      castor::tape::tpcp::TpcpCommand::usage(std::cerr, TPCPPROGRAMNAME);
      std::cerr << std::endl;
      return 1;
    }

    // If debug, then display parsed command-line arguments
    if(m_parsedCommandLine.debugOptionSet) {
      std::ostream &os = std::cout;

      os << std::endl;
      os << "===================" << std::endl;
      os << "Parsed command-line" << std::endl;
      os << "===================" << std::endl;
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

/*
    // Get the port range to be used
    int   lowPort  = LOW_CLIENT_PORT_RANGE;
    int   highPort = HIGH_CLIENT_PORT_RANGE;
    char* sport;
    if((sport = getconfent((char *)CLIENT_CONF,(char *)LOWPORT_CONF,0)) != 0) {
      lowPort = castor::System::porttoi(sport);
    }
    if((sport = getconfent((char *)CLIENT_CONF,(char *)HIGHPORT_CONF,0)) != 0) {
      highPort = castor::System::porttoi(sport);
    }

    // Bind the socket
    m_callbackSocket.bind(lowPort, highPort);
    m_callbackSocket.listen();

    unsigned short port = 0;
    unsigned long  ip   = 0;
    m_callbackSocket.getPortIp(port, ip);

    int VolReqID = 0;
    // Send the request to read a tape
    std::cerr<<"vdqm_SendAggregatorVolReq (NULL, " << VolReqID << ", " 
      << m_parsedCommandLine.vid <<", "<< m_parsedCommandLine.dgn
      <<", NULL, NULL, "
      << actionToString(m_parsedCommandLine.action) <<", "
      <<port<< ")"<<std::endl;
    // *nv, *reqID, *VID, *dgn, *server, *unit, mode, client_port
    int rc = 0;
    rc = vdqm_SendAggregatorVolReq(NULL, &VolReqID,  m_parsedCommandLine.vid, 
       m_parsedCommandLine.dgn, NULL, NULL, 
       actionToString(m_parsedCommandLine.action), port);

    if ( rc == -1 ) {
      char codeStr[STRERRORBUFLEN];
      strerror_r(errno, codeStr, sizeof(codeStr));
      TAPE_THROW_EX(castor::exception::Internal,
        ": vdqm_SendAggregatorVolReq()"
        ": Error=" << codeStr);
    }
  
    fprintf(stdout,
      "Volume request successfully submitted. Volume request ID:%d\n", VolReqID);

    //.........................................................................
    // Wait for the Aggreator messege 
    std::auto_ptr<castor::io::ServerSocket> socket(
      (castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj(socket->readObject());
    std::cerr<<"Obj type: " << obj->type()<<std::endl;
    // OBJ_VolumeRequest = 168

    //.........................................................................
    // Create and send the Volume Request message to the Aggregator 
    castor::tape::tapegateway::Volume volumeMsg; 

    volumeMsg.setVid(vid);
    volumeMsg.setMode(mode);
    volumeMsg.setLabel("aul\0");
    volumeMsg.setTransactionId(VolReqID);
    volumeMsg.setDensity("1000GC\0");

    socket->sendObject(volumeMsg);

    // close the socket!!
    //delete(socket);  //
    delete(socket.release());

    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj2(socket->readObject());
    std::cerr<<"Obj type: " << obj2->type()<<std::endl;
    // OBJ_FileToRecallRequest = 166


    // Loop over all the files to recall

    {
      //.........................................................................
      // Send a FileToRecall message to the Aggregator
      castor::tape::tapegateway::FileToRecall fileToRecallMsg;
  
      fileToRecallMsg.setTransactionId(VolReqID);
      fileToRecallMsg.setNshost("castorns\0");
      fileToRecallMsg.setFileid(320723286);
      fileToRecallMsg.setFseq(5);
      fileToRecallMsg.setPositionCommandCode(
        castor::tape::tapegateway::PositionCommandCode(3));
    
      fileToRecallMsg.setPath("lxc2disk15.cern.ch:/tmp/volume_I02000_file_n5");
      fileToRecallMsg.setBlockId0(0);
      fileToRecallMsg.setBlockId1(0);
      fileToRecallMsg.setBlockId2(0);
      fileToRecallMsg.setBlockId3(41);
    
      socket->sendObject(fileToRecallMsg);

      // close the socket!!
      delete(socket.release());


      //.........................................................................
      // Wait for the Aggreator messege 
      socket.reset((castor::io::ServerSocket*)waitForCallBack());

      std::auto_ptr<castor::IObject>  obj3(socket->readObject());
      std::cerr<<"Obj type: " << obj3->type()<<std::endl;
      // OBJ_FileToRecallRequest = 166
    }


    //.........................................................................
    // Sent a NoMoreFile message
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setTransactionId(VolReqID);

    socket->sendObject(noMore);

    delete(socket.release());


    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj4(socket->readObject());
    std::cerr<<"Obj type: " << obj4->type()<<std::endl;
    //OBJ_FileRecalledNotification = 163

    //.........................................................................
    // Notification Acknowledge 
    castor::tape::tapegateway::NotificationAcknowledge notificationAcknowledge;
    notificationAcknowledge.setTransactionId(VolReqID);
    
    socket->sendObject(notificationAcknowledge);

    delete(socket.release());

    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj5(socket->readObject());
    std::cerr<<"Obj type: " << obj5->type()<<std::endl;
    // OBJ_EndNotification = 172

    
    //.........................................................................
    // Notification Acknowledge
    socket->sendObject(notificationAcknowledge);

    delete(socket.release());

    //.........................................................................

    std::cerr<<"We have probably recalled a file (lxc2disk15.cern.ch:/tmp/volume_I02000_file_n5)"<<std::endl;


    



*/
  } catch (castor::exception::Exception &ex) {
    std::cerr << std::endl << "Failed to start daemon: "
      << ex.getMessage().str() << std::endl << std::endl;
    //castor::tape::aggregator::AggregatorDaemon::usage(std::cerr);
    std::cerr << std::endl;
    return 1;
  }

  return 0;
}


//------------------------------------------------------------------------------
// writeTapeFseqRangeList
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeTapeFseqRangeList(std::ostream &os,
  castor::tape::tpcp::TpcpCommand::Uint32RangeList &list) {
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
// writeParsedCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::TpcpCommand::writeParsedCommandLine(std::ostream &os) {
  os << "debugOptionSet="
     << utils::boolToString(m_parsedCommandLine.debugOptionSet)
     << std::endl;

  os << "helpOptionSet ="
     << utils::boolToString(m_parsedCommandLine.helpOptionSet)
     << std::endl;

  os << "action        ="
     << m_parsedCommandLine.action.str()
     << std::endl;

  os << "vid           =";
  if(m_parsedCommandLine.vid == NULL) {
    os << "NULL";
  } else {
    os << "\"" << m_parsedCommandLine.vid << "\"";
  }
  os << std::endl;

  os << "tapeFseqRanges=";
  writeTapeFseqRangeList(os, m_parsedCommandLine.tapeFseqRanges);
  os << std::endl;
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
        ex.getMessage() << "Invalid range string: '" << boundaryStrs[0]
          << "': The lower boundary should be an unsigned integer";
        throw ex;
      }

      range.lower = atoi(boundaryStrs[0].c_str());

      // If "m-"
      if(boundaryStrs[1] == "") {
        // Inifinity (or until the end of tape) is represented by 0
        range.upper = 0;
      // Else "m-n"
      } else {
        // Parse the "n" of "m-n"
        if(!utils::isValidUInt(boundaryStrs[1].c_str())) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << boundaryStrs[1]
            << "': The upper boundary should be an unsigned integer";
          throw ex;
        }
        range.upper = atoi(boundaryStrs[1].c_str());
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
