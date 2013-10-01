/******************************************************************************
 *                 castor/tape/mediachanger/MountAcsCmd.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/mediachanger/MountAcsCmd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <getopt.h>
#include <iostream>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::MountAcsCmd::MountAcsCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs), m_defaultQueryInterval(10),
  m_defaultTimeout(600) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::MountAcsCmd::~MountAcsCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::mediachanger::MountAcsCmd::main(const int argc,
  char *const *const argv) throw() {
  try {
    m_cmdLine = parseCmdLine(argc, argv);
  } catch(castor::exception::InvalidArgument &ia) {
    m_err << "Aborting: Invalid command-line: " << ia.getMessage().str() <<
      std::endl;
    m_err << std::endl;
    usage(m_err);
    return 1;
  } catch(castor::exception::MissingOperand &mo) {
    m_err << "Aborting: Missing operand: " << mo.getMessage().str() <<
      std::endl;
    m_err << std::endl;
    usage(m_err);
    return 1;
  } catch(castor::exception::Internal &ie) {
    m_err << "Aborting: Internal error: " << ie.getMessage().str() <<
      std::endl;
    return 1;
  }

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    usage(m_out);
    return 0;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "readonly = " << bool2Str(m_cmdLine.readOnly) << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;
  m_dbg << "DRIVE = " << m_acs.driveId2Str(m_cmdLine.driveId) << std::endl;

  try {
    syncMount();
  } catch(castor::exception::Exception &ex) {
    m_err << "Aborting: " << ex.getMessage().str() << std::endl;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
castor::tape::mediachanger::MountAcsCmdLine
  castor::tape::mediachanger::MountAcsCmd::parseCmdLine(
  const int argc, char *const *const argv)
  throw(castor::exception::Internal, castor::exception::InvalidArgument,
    castor::exception::MissingOperand) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"readonly" , 0, NULL, 'r'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };
  MountAcsCmdLine cmdLine;
  char c;

  // Set the query option to the default value
  cmdLine.queryInterval = m_defaultQueryInterval;

  // Set timeout option to the default value
  cmdLine.timeout = m_defaultTimeout;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;
  while((c = getopt_long(argc, argv, ":dhq:rt:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      cmdLine.debug = true;
      break;
    case 'h':
      cmdLine.help = true;
      break;
    case 'q':
      cmdLine.queryInterval = atoi(optarg);
      if(0 >= cmdLine.queryInterval) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Query value must be an integer greater than 0"
          ": value=" << cmdLine.queryInterval;
        throw ex;
      }
      break;
    case 'r':
      cmdLine.readOnly = TRUE;
      break;
    case 't':
      cmdLine.timeout = atoi(optarg);
      if(0 >= cmdLine.timeout) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Timeout value must be an integer greater than 0"
          ": value=" << cmdLine.timeout;
        throw ex;
      }
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
        ex.getMessage() <<
          "getopt_long returned the following unknown value: 0x" <<
          std::hex << (int)c;
        throw ex;
      }
    } // switch (c)
  } // while ((c = getopt_long( ... )) != -1)

  // There is no need to continue parsing when the help option is set
  if(cmdLine.help) {
    return cmdLine;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  // Check that both VID and DRIVE has been specified
  if(nbArgs < 2) {
    castor::exception::MissingOperand ex;
    ex.getMessage() << "Both VID and DRIVE must be specified";
    throw ex;
  }

  // Parse the VID command-line argument
  cmdLine.volId = m_acs.str2Volid(argv[optind]);

  // Move on to the next command-line argument
  optind++;

  // Parse the DRIVE command-line argument
  cmdLine.driveId = m_acs.str2DriveId(argv[optind]);

  return cmdLine;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::usage(std::ostream &os)
  const throw() {
  os <<
  "Usage:\n"
  "  mountacs [options] VID DRIVE\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be mounted.\n"
  "  DRIVE  The ID of the drive into which the volume is to be mounted.\n"
  "         Drive ID format is ACS:LSM:panel:transport\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "  -h|--help             Print this help message and exit.\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << m_defaultQueryInterval << ".\n"
  "  -r|--readOnly         Request the volume is mounted for read-only access\n"
  "  -t|--timeout SECONDS  Time to wait for the mount to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS is "
    << m_defaultTimeout << ".\n"
  "\n"
  "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// syncMount
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::syncMount()
  throw(castor::exception::MountFailed) {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  sendMountRequest(requestSeqNumber);
  requestResponsesUntilFinal(requestSeqNumber, buf);
  processMountResponse(buf);
}

//------------------------------------------------------------------------------
// sendMountRequest
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::sendMountRequest(
  const SEQ_NO seqNumber) throw(castor::exception::MountFailed) {
  const LOCKID lockId = 0; // No lock
  const BOOLEAN bypass = FALSE;

  m_dbg << "Calling Acs::mount()" << std::endl;
  const STATUS s = m_acs.mount(seqNumber, lockId, m_cmdLine.volId,
    m_cmdLine.driveId, m_cmdLine.readOnly, bypass);
  m_dbg << "Acs::mount() returned " << acs_status(s) << std::endl;

  if(STATUS_SUCCESS != s) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Failed to send request to mount volume " <<
      m_cmdLine.volId.external_label << " into drive " <<
      m_acs.driveId2Str(m_cmdLine.driveId) << ": readOnly=" <<
      (m_cmdLine.readOnly ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::requestResponsesUntilFinal(
  const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw (castor::exception::MountFailed) {
  ACS_RESPONSE_TYPE responseType = RT_NONE;
  int elapsedTime = 0;
  do {
    const int remainingTime = m_cmdLine.timeout - elapsedTime;
    const int responseTimeout = remainingTime > m_cmdLine.queryInterval ?
      m_cmdLine.queryInterval : remainingTime;

    const time_t startTime = time(NULL);
    responseType = requestResponse(responseTimeout, requestSeqNumber, buf);
    elapsedTime += time(NULL) - startTime;

    if(RT_ACKNOWLEDGE == responseType) {
      m_dbg << "Received RT_ACKNOWLEDGE" << std::endl;
    }

    if(elapsedTime >= m_cmdLine.timeout) {
      castor::exception::MountFailed ex;
      ex.getMessage() << "Timed out after " << m_cmdLine.timeout << " seconds";
      throw(ex);
    }
  } while(RT_FINAL != responseType);

  m_dbg << "Received RT_FINAL" << std::endl;
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE castor::tape::mediachanger::MountAcsCmd::
  requestResponse(const int timeout, const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::MountFailed) {
  SEQ_NO responseSeqNumber = 0;
  REQ_ID reqId = (REQ_ID)0;
  ACS_RESPONSE_TYPE responseType = RT_NONE;

  m_dbg << "Calling Acs::response()" << std::endl;
  const STATUS s = m_acs.response(timeout, responseSeqNumber, reqId,
    responseType, buf);
  m_dbg << "Acs::response() returned " << acs_status(s) << std::endl;

  switch(s) {
  case STATUS_SUCCESS:
    checkResponseSeqNumber(requestSeqNumber, responseSeqNumber);
    return responseType;
  case STATUS_PENDING:
    return RT_NONE;
  default:
    castor::exception::MountFailed ex;
    ex.getMessage() << "Failed to request response: " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkSeqNumber
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::checkResponseSeqNumber(
  const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber)
  throw(castor::exception::MountFailed) {
  if(requestSeqNumber != responseSeqNumber) {
    castor::exception::MountFailed ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// processMountResponse
//------------------------------------------------------------------------------
void castor::tape::mediachanger::MountAcsCmd::processMountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::MountFailed) {
  const ACS_MOUNT_RESPONSE *const msg = (ACS_MOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->mount_status) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Status of mount response is not success: " <<
      acs_status(msg->mount_status);
    throw(ex);
  }
}
