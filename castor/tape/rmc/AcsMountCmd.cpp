/******************************************************************************
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

#include "castor/tape/rmc/AcsMountCmd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <getopt.h>
#include <iostream>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcsMountCmd::AcsMountCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs), m_defaultQueryInterval(10),
  m_defaultTimeout(600) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::AcsMountCmd::~AcsMountCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::rmc::AcsMountCmd::main(const int argc,
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
  } catch(castor::exception::Exception &ie) {
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
castor::tape::rmc::AcsMountCmdLine
  castor::tape::rmc::AcsMountCmd::parseCmdLine(
  const int argc, char *const *const argv)
  throw(castor::exception::Exception, castor::exception::InvalidArgument,
    castor::exception::MissingOperand) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"readonly" , 0, NULL, 'r'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };
  AcsMountCmdLine cmdLine;
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
        castor::exception::Exception ex;
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
void castor::tape::rmc::AcsMountCmd::usage(std::ostream &os)
  const throw() {
  os <<
  "Usage:\n"
  "\n"
  "  castor-tape-acs-mount [options] VID DRIVE\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be mounted.\n"
  "  DRIVE  The ID of the drive into which the volume is to be mounted.\n"
  "         The format of DRIVE is:\n"
  "\n"
  "             ACS:LSM:panel:transport\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help             Print this help message and exit.\n"
  "\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << m_defaultQueryInterval << ".\n"
  "\n"
  "  -r|--readOnly         Request the volume is mounted for read-only access\n"
  "\n"
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
void castor::tape::rmc::AcsMountCmd::syncMount()
   {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendMountRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, m_cmdLine.queryInterval,
      m_cmdLine.timeout);
    processMountResponse(buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::MountFailed mf;
    mf.getMessage() << "Failed to mount volume " <<
      m_cmdLine.volId.external_label << ": " << ex.getMessage().str();
    throw mf;
  }
}

//------------------------------------------------------------------------------
// sendMountRequest
//------------------------------------------------------------------------------
void castor::tape::rmc::AcsMountCmd::sendMountRequest(
  const SEQ_NO seqNumber)  {
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
// processMountResponse
//------------------------------------------------------------------------------
void castor::tape::rmc::AcsMountCmd::processMountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
   {
  const ACS_MOUNT_RESPONSE *const msg = (ACS_MOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->mount_status) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Status of mount response is not success: " <<
      acs_status(msg->mount_status);
    throw ex;
  }
}
