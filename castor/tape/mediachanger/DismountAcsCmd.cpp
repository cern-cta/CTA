/******************************************************************************
 *                 castor/tape/mediachanger/DismountAcsCmd.cpp
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

#include "castor/tape/mediachanger/DismountAcsCmd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <getopt.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::DismountAcsCmd::DismountAcsCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs), m_defaultQueryInterval(10),
  m_defaultTimeout(600) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::DismountAcsCmd::~DismountAcsCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::mediachanger::DismountAcsCmd::main(const int argc,
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

  m_dbg << "force = " << (m_cmdLine.force ? "TRUE" : "FALSE") << std::endl;
  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;
  m_dbg << "DRIVE = " << m_acs.driveId2Str(m_cmdLine.driveId) << std::endl;

  try {
    syncDismount();
  } catch(castor::exception::Exception &ex) {
    m_err << "Aborting: " << ex.getMessage().str() << std::endl;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DismountAcsCmd::usage(std::ostream &os)
  const throw() {
  os <<
  "Usage:\n"
  "  dismountacs [options] VID DRIVE\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be dismounted.\n"
  "  DRIVE  The drive from which the volume is to be dismounted.\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "  -f|--force            Force the dismount.\n"
  "  -h|--help             Print this help message and exit.\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << m_defaultQueryInterval << ".\n"
  "  -t|--timeout SECONDS  Time to wait for the dismount to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS is "
    << m_defaultTimeout << ".\n"
  "\n"
  "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
castor::tape::mediachanger::DismountAcsCmdLine
  castor::tape::mediachanger::DismountAcsCmd::parseCmdLine(
  const int argc, char *const *const argv)
  throw(castor::exception::Internal, castor::exception::InvalidArgument,
    castor::exception::MissingOperand) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"force", 0, NULL, 'f'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL   , 0, NULL,  0 }
  };
  DismountAcsCmdLine cmdLine;
  char c;

  // Set the query option to the default value
  cmdLine.queryInterval = m_defaultQueryInterval;

  // Set timeout option to the default value
  cmdLine.timeout = m_defaultTimeout;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;
  while((c = getopt_long(argc, argv, ":dfhq:t:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      cmdLine.debug = true;
      break;
    case 'f':
      cmdLine.force = TRUE;
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
  const int nbArgs = argc-optind;

  // Check that both VID and DRIVE has been specified
  if(nbArgs < 2) {
    castor::exception::MissingOperand ex;

    ex.getMessage() <<
      "Both VID and DRIVE must be specified";

    throw ex;
  }

  // Parse the volume identifier of the command-line argument
  cmdLine.volId = m_acs.str2Volid(argv[optind]);

  // Move on to the next command-line argument
  optind++;

  // Parse the drive-identifier command-line argument
  cmdLine.driveId = m_acs.str2DriveId(argv[optind]);

  return cmdLine;
}

//------------------------------------------------------------------------------
// syncDismount
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DismountAcsCmd::syncDismount()
  throw(castor::exception::DismountFailed) {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendDismountRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, m_cmdLine.queryInterval,
      m_cmdLine.timeout);
    processDismountResponse(buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::DismountFailed df;
    df.getMessage() << "Failed to dismount volume " <<
      m_cmdLine.volId.external_label << ": " << ex.getMessage().str();
    throw df;
  }
}

//------------------------------------------------------------------------------
// sendDismountRequest
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DismountAcsCmd::sendDismountRequest(
  const SEQ_NO seqNumber) throw (castor::exception::DismountFailed) {
  const LOCKID lockId = 0; // No lock

  m_dbg << "Calling Acs::dismount()" << std::endl;
  const STATUS s = m_acs.dismount(seqNumber, lockId, m_cmdLine.volId,
    m_cmdLine.driveId, m_cmdLine.force);
  m_dbg << "Acs::dismount() returned " << acs_status(s) << std::endl;
  if(STATUS_SUCCESS != s) {
    castor::exception::DismountFailed ex;
    ex.getMessage() << "Failed to send request to dismount volume " <<
      m_cmdLine.volId.external_label << " from drive " <<
      m_acs.driveId2Str(m_cmdLine.driveId) << ": force=" <<
      (m_cmdLine.force ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// processDismountResponse
//------------------------------------------------------------------------------
void castor::tape::mediachanger::DismountAcsCmd::processDismountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::DismountFailed) {
  const ACS_DISMOUNT_RESPONSE *const msg = (ACS_DISMOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->dismount_status) {
    castor::exception::DismountFailed ex;
    ex.getMessage() << "Status of dismount response is not success: " <<
      acs_status(msg->dismount_status);
    throw(ex);
  }
}
