/******************************************************************************
 *                 castor/tape/tpcp/DumpTpCommand.cpp
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
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/DumpNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/DumpTpCommand.hpp"
#include "castor/tape/tpcp/Helper.hpp"

#include <errno.h>
#include <getopt.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::DumpTpCommand::DumpTpCommand() throw() {

  // Register the Aggregator message handler member functions
  registerMsgHandler(OBJ_DumpNotification,
    &DumpTpCommand::handleDumpNotification, this);
  registerMsgHandler(OBJ_EndNotification,
    &DumpTpCommand::handleEndNotification, this);
  registerMsgHandler(OBJ_EndNotificationErrorReport,
    &DumpTpCommand::handleEndNotificationErrorReport, this);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::DumpTpCommand::~DumpTpCommand() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::usage(std::ostream &os,
  const char *const programName) throw() {
  os <<
    "Usage:\n"
    "\t" << programName << " VID [OPTIONS]\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID  The VID of the tape to be dumped.\n"
    "\n"
    "Options:\n"
    "\n"
    "\t-d, --debug         Print debug information.\n"
    "\t-h, --help          Print this help and exit.\n"
    "\t-s, --server server Specifies the tape server to be used therefore\n"
    "\t                    overriding the drive scheduling of the VDQM.\n"
    "\t-B, --maxbytes      The number of bytes per block to be dumped (0\n"
    "\t                    means all bytes.  The default is 320.\n"
    "\t-b, --blksize       The maximum block size in bytes.  The default is\n"
    "\t                    65536.\n"
    "\t-f, --startfile     The tape file sequence number of the start file\n"
    "\t-g, --maxfiles      The number of files to be dumped (0 means all\n"
    "\t                    files).  The default is all files for 3420/3480/\n"
    "\t                    3490, and 1 file for Exabyte/DLT/IBM3590.\n"
    "\t-m, --fromBlock     Start block per file.  The default is 1.\n"
    "\t-n, --toBlock       End block per file. The default is 1.\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

  m_cmdLine.action = Action::dump;

  static struct option longopts[] = {
    {"debug"    , 0, NULL, 'd'},
    {"help"     , 0, NULL, 'h'},
    {"server"   , 1, NULL, 's'},
    {"maxbytes" , 1, NULL, 'B'},
    {"blksize"  , 1, NULL, 'b'},
    {"startfile", 1, NULL, 'f'},
    {"maxfiles" , 1, NULL, 'g'},
    {"fromBlock", 1, NULL, 'm'},
    {"toBlock"  , 1, NULL, 'n'},
    {NULL       , 0, NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":dhs:B:b:f:g:m:n:", longopts, NULL))
    != -1) {

    switch (c) {
    case 'd':
      m_cmdLine.debugSet = true;
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

    case 'B':  // maxbytes
      std::cout<<"-B:"<<optarg<<std::endl; 
      m_cmdLine.dumpTapeMaxBytes=atoi(optarg);
      break;

    case 'b':  // blksize
      std::cout<<"-b:"<<optarg<<std::endl; 
      m_cmdLine.dumpTapeBlockSize=atoi(optarg);
      break;

    case 'f':  // startfile
      std::cout<<"-f:"<<optarg<<std::endl;
      m_cmdLine.dumpTapeStartFile=atoi(optarg);
      break;

    case 'g':  // maxfiles
      std::cout<<"-g:"<<optarg<<std::endl;
      m_cmdLine.dumpTapeMaxFiles=atoi(optarg);
      break;

    case 'm':  // fromBlock
      std::cout<<"-m:"<<optarg<<std::endl;
      m_cmdLine.dumpTapeFromBlock=atoi(optarg);
      break;

    case 'n':  // toBlock
      std::cout<<"-n:"<<optarg<<std::endl;
      m_cmdLine.dumpTapeToBlock=atoi(optarg);
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

  // The number of expected command-line arguments is 1
  const int expectedNbArgs = 1;

  // Check the number of command-line arguments are present
  if(argc-optind < expectedNbArgs){
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tWrong number of command-line arguments: Actual=" <<
      argc-optind << " Expected=" << expectedNbArgs;

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
}


//------------------------------------------------------------------------------
// performTransfer
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::performTransfer()
  throw (castor::exception::Exception) {

  // Spin in the dispatch message loop until there is no more work
  while(dispatchMessage()) {
    // Do nothing
  }

  std::ostream &os = std::cout;

  time_t now = time(NULL);
  utils::writeTime(os, now, TIMEFORMAT);
  os << ": Finished dumping tape" << std::endl
     << std::endl;
}


//------------------------------------------------------------------------------
// handleDumpNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handleDumpNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {


  tapegateway::DumpNotification *msg = NULL;

  castMessage(obj, msg, sock);

  // Write the message to standard out
  std::ostream &os = std::cout;
  os << msg->message();

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
bool castor::tape::tpcp::DumpTpCommand::handleEndNotification(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handleEndNotificationErrorReport(
  castor::IObject *obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotificationErrorReport(obj,sock);
}
