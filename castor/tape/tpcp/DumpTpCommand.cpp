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
#include "castor/tape/tapegateway/DumpParameters.hpp"
#include "castor/tape/tapegateway/DumpParametersRequest.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/DumpTpCommand.hpp"
#include "castor/tape/tpcp/Helper.hpp"
#include "h/Ctape_constants.h"
#include "h/Cupv_api.h"
#include "h/rtcp_constants.h"

#include <errno.h>
#include <getopt.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::DumpTpCommand::DumpTpCommand() throw():
  TpcpCommand("dumptp") {
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
void castor::tape::tpcp::DumpTpCommand::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " VID [OPTIONS]\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID  The VID of the tape to be dumped.\n"
    "\n"
    "Options:\n"
    "\n"
    "\t-d, --debug           Turn on the printing of debug information.\n"
    "\t-h, --help            Print this help and exit.\n"
    "\t-s, --server server   Specifies the tape server to be used, therefore\n"
    "\t                      overriding the drive scheduling of the VDQM.\n"
    "\t-B, --maxbytes n      The number of bytes per block to be dumped.\n"
    "\t                      This should be an unsigned integer greater than\n"
    "\t                      or equal to 0, where 0 means all bytes.  The\n"
    "\t                      default is 320.\n"
    "\t-b, --blksize n       The maximum block size in bytes.  This should be\n"
    "\t                      an unsigned integer greater than 0.  The default\n"
    "\t                      is " << DEFAULTDUMPBLOCKSIZE << ".\n"
    "\t-f, --fromfile seq    The tape file sequence number to start from.\n"
    "\t                      This should be an unsigned integer greater than\n"
    "\t                      0.  The default is 1.\n"
    "\t-g, --maxfiles n      The number of files to be dumped.  This should\n"
    "\t                      be an unsigned integer greater than or equal to\n"
    "\t                      0, where 0 means all files.  The default is 1.\n"
    "\t-m, --fromblock block Start block per file.  This should be an\n"
    "\t                      unsigned integer greater than 0.  The default is\n"
    "\t                      1.\n"
    "\t-n, --toblock block   End block per file.  This should be an unsigned\n"
    "\t                      integer greater than 0.  The default is 1.\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

  static struct option longopts[] = {
    {"debug"    , 0, NULL, 'd'},
    {"help"     , 0, NULL, 'h'},
    {"server"   , 1, NULL, 's'},
    {"maxbytes" , 1, NULL, 'B'},
    {"blksize"  , 1, NULL, 'b'},
    {"fromfile" , 1, NULL, 'f'},
    {"maxfiles" , 1, NULL, 'g'},
    {"fromblock", 1, NULL, 'm'},
    {"toblock"  , 1, NULL, 'n'},
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
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -B,--maxbytes argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      m_cmdLine.dumpTapeMaxBytes = atoi(optarg);
      break;

    case 'b':  // blksize
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -b,--blksize argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      m_cmdLine.dumpTapeBlockSize = atoi(optarg);
      if(m_cmdLine.dumpTapeBlockSize < 1) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -b,--blksize argument must be greater than 0"
          ": Actual=" << optarg;
        throw ex;
      }
      break;

    case 'f':  // fromfile
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -f,--fromfile argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      if((m_cmdLine.dumpTapeFromFile = atoi(optarg)) < 1) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -f,--fromfile argument must be greater than 0"
          ": Actual=" << optarg;
        throw ex;
      }
      break;

    case 'g':  // maxfiles
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -g,--maxfiles argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      m_cmdLine.dumpTapeMaxFiles = atoi(optarg);
      break;

    case 'm':  // fromblock
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -m,--fromblock argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      if((m_cmdLine.dumpTapeFromBlock = atoi(optarg)) < 1) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -m,--fromblock argument must be greater than 0"
          ": Actual=" << optarg;
        throw ex;
      }
      break;

    case 'n':  // toblock
      if(!utils::isValidUInt(optarg)) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -n,--toblock argument must be an unsigned integer"
          ": Actual=" << optarg;
        throw ex;
      }
      if((m_cmdLine.dumpTapeToBlock = atoi(optarg)) < 1) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "\tThe -n,--toblock argument must be greater than 0"
          ": Actual=" << optarg;
        throw ex;
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
  if(m_cmdLine.helpSet) {
    return;
  }

  if(m_cmdLine.dumpTapeFromBlock > m_cmdLine.dumpTapeToBlock) {
    castor::exception::InvalidArgument ex;
    ex.getMessage() <<
      "\tThe -n,--toblock argument must be greater than or equal to the \n"
      "\t-m,--fromblock argument"
      ": fromblock="<< m_cmdLine.dumpTapeFromBlock <<
      " toblock=" << m_cmdLine.dumpTapeToBlock;
      throw ex;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc-optind;

  // Check the VID has been specified
  if(nbArgs < 1) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tThe VID has not been specified";

    throw ex;
  }

  // Check there are no other non-option ARGV-elements
  if(nbArgs > 1) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tToo many command-line arguments: Actual=" << nbArgs
      << " Expecting only a VID";

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
// checkAccessToDisk
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::checkAccessToDisk()
  const throw (castor::exception::Exception) {
  // There are no disk files when making a dump of tape, therefore throw no
  // exceptions.
}


//------------------------------------------------------------------------------
// checkAccessToTape
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::checkAccessToTape()
  const throw (castor::exception::Exception) {
  const bool userIsTapeOperator =
    Cupv_check(m_userId, m_groupId, m_hostname, "TAPE_SERVERS",
      P_TAPE_OPERATOR) == 0 ||
    Cupv_check(m_userId, m_groupId, m_hostname, NULL          ,
      P_TAPE_OPERATOR) == 0;

  // Only tape-operators can dump disabled tapes
  if(m_vmgrTapeInfo.status & DISABLED) {
    if(!userIsTapeOperator) {
      castor::exception::Exception ex(ECANCELED);
      std::ostream &os = ex.getMessage();
      os << "Tape is not available for dumping"
        ": Tape is DISABLED and user is not a tape-operator";
      throw ex;
    }
  }

  if(m_vmgrTapeInfo.status & EXPORTED ||
     m_vmgrTapeInfo.status & ARCHIVED) {
     castor::exception::Exception ex(ECANCELED);
     std::ostream &os = ex.getMessage();
     os << "Tape is not available for dumping"
       ": Tape is";
     if(m_vmgrTapeInfo.status & EXPORTED) os << " EXPORTED";
     if(m_vmgrTapeInfo.status & ARCHIVED) os << " ARCHIVED";
     throw ex;
  }
}


//------------------------------------------------------------------------------
// requestDriveFromVdqm
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::requestDriveFromVdqm(
  char *const tapeServer) throw(castor::exception::Exception) {
  TpcpCommand::requestDriveFromVdqm(WRITE_DISABLE, tapeServer);
}


//------------------------------------------------------------------------------
// sendVolumeToTapeBridge
//------------------------------------------------------------------------------
void castor::tape::tpcp::DumpTpCommand::sendVolumeToTapeBridge(
  const tapegateway::VolumeRequest &volumeRequest,
  castor::io::AbstractTCPSocket    &connection)
  const throw(castor::exception::Exception) {
  castor::tape::tapegateway::Volume volumeMsg;
  volumeMsg.setVid(m_vmgrTapeInfo.vid);
  volumeMsg.setClientType(castor::tape::tapegateway::DUMP_TP);
  volumeMsg.setMode(castor::tape::tapegateway::DUMP);
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
void castor::tape::tpcp::DumpTpCommand::performTransfer()
  throw (castor::exception::Exception) {

  // Spin in the wait for message and dispatch handler loop until there is no
  // more work
  while(waitForMsgAndDispatchHandler()) {
    // Do nothing
  }

  std::ostream &os = std::cout;

  time_t now = time(NULL);
  utils::writeTime(os, now, TIMEFORMAT);
  os << " Finished dumping tape" << std::endl
     << std::endl;
}


//------------------------------------------------------------------------------
// dispatchMsgHandler
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::dispatchMsgHandler(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {
  switch(obj->type()) {
  case OBJ_DumpParametersRequest:
    return handleDumpParametersRequest(obj, sock);
  case OBJ_DumpNotification:
    return handleDumpNotification(obj, sock);
  case OBJ_EndNotification:
    return handleEndNotification(obj, sock);
  case OBJ_EndNotificationErrorReport:
    return handleEndNotificationErrorReport(obj, sock);
  case OBJ_PingNotification:
    return handlePingNotification(obj, sock);
  default:
    {
      std::stringstream oss;

      oss <<
        "Received unexpected tapebridge message"
        ": Message type = " << utils::objectTypeToString(obj->type());

      const uint64_t tapebridgeTransactionId = 0; // Unknown transaction ID
      sendEndNotificationErrorReport(tapebridgeTransactionId, EBADMSG,
        oss.str(), sock);

      TAPE_THROW_CODE(EBADMSG,
        ": Received unexpected tapebridge message "
        ": Message type = " << utils::objectTypeToString(obj->type()));
    }
  }
}


//------------------------------------------------------------------------------
// handleDumpParametersRequest
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handleDumpParametersRequest(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::DumpParametersRequest *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  // Create DumpParameters message for the tapebridge
  tapegateway::DumpParameters dumpParameters;
  dumpParameters.setMountTransactionId(m_volReqId);
  dumpParameters.setAggregatorTransactionId(msg->aggregatorTransactionId());
  dumpParameters.setMaxBytes(m_cmdLine.dumpTapeMaxBytes);
  dumpParameters.setBlockSize(m_cmdLine.dumpTapeBlockSize);
  dumpParameters.setConverter(ASCCONV);
  dumpParameters.setErrAction(m_cmdLine.dumpTapeErrAction);
  dumpParameters.setStartFile(m_cmdLine.dumpTapeFromFile);
  dumpParameters.setMaxFile(m_cmdLine.dumpTapeMaxFiles);
  dumpParameters.setFromBlock(m_cmdLine.dumpTapeFromBlock);
  dumpParameters.setToBlock(m_cmdLine.dumpTapeToBlock);

  // Send the DumpParameters message to the tapebridge
  sock.sendObject(dumpParameters);

  Helper::displaySentMsgIfDebug(dumpParameters, m_cmdLine.debugSet);

  return true;
}


//------------------------------------------------------------------------------
// handleDumpNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handleDumpNotification(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  tapegateway::DumpNotification *msg = NULL;

  castMessage(obj, msg, sock);
  Helper::displayRcvdMsgIfDebug(*msg, m_cmdLine.debugSet);

  // Write the message to standard out
  std::ostream &os = std::cout;
  os << msg->message();

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
bool castor::tape::tpcp::DumpTpCommand::handleEndNotification(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotification(obj, sock);
}


//------------------------------------------------------------------------------
// handleEndNotificationErrorReport
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handleEndNotificationErrorReport(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handleEndNotificationErrorReport(obj,sock);
}


//------------------------------------------------------------------------------
// handlePingNotification
//------------------------------------------------------------------------------
bool castor::tape::tpcp::DumpTpCommand::handlePingNotification(
  castor::IObject *const obj, castor::io::AbstractSocket &sock)
  throw(castor::exception::Exception) {

  return TpcpCommand::handlePingNotification(obj,sock);
}
