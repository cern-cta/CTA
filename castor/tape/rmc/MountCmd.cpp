/******************************************************************************
 *                 castor/tape/rmc/MountCmd.cpp
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

#include "castor/tape/rmc/MountCmd.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rmc_api.h"
#include "h/serrno.h"

#include <getopt.h>
#include <iostream>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::rmc::MountCmd::MountCmd(std::istream &inStream,
  std::ostream &outStream, std::ostream &errStream) throw():
  Cmd(inStream, outStream, errStream), m_defaultTimeout(600) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::rmc::MountCmd::~MountCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::rmc::MountCmd::main(const int argc,
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

  m_dbg << "readonly = " << bool2Str(m_cmdLine.readOnly) << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId << std::endl;
  m_dbg << "DRIVE = " << m_cmdLine.driveId << std::endl;

  const int rmc_mnt_rc = rmc_mnt(m_cmdLine.volId.c_str(),
    m_cmdLine.driveId.c_str());
  const int rmc_mnt_serrno = serrno;
  switch(rmc_mnt_rc) {
  case 1:
    m_out <<
      "You have requested a manual mount.  The castor-tape-mount command\n"
      " does nothing for this type of mount." << std::endl;
    return 0;
  case 0:
    m_out << m_cmdLine.volId << " has been mounted" << std::endl;
    return 0;
  case -1:
    m_err << "Failed to mount " << m_cmdLine.volId << ": " <<
      sstrerror(rmc_mnt_serrno) << std::endl;
    return -1;
  default:
    m_err << "Internal error: rmc_mnt() returned the unexpected value of " <<
      rmc_mnt_rc << std::endl;
    return -1;
  }
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
castor::tape::rmc::MountCmdLine castor::tape::rmc::MountCmd::parseCmdLine(
  const int argc, char *const *const argv)
  throw(castor::exception::Exception, castor::exception::InvalidArgument,
    castor::exception::MissingOperand) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"readonly", 0, NULL, 'r'},
    {"timeout", required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };
  MountCmdLine cmdLine;
  char c;

  // Set timeout option to the default value
  cmdLine.timeout = m_defaultTimeout;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;
  while((c = getopt_long(argc, argv, ":dhrt:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      cmdLine.debug = true;
      break;
    case 'h':
      cmdLine.help = true;
      break;
    case 'r':
      cmdLine.readOnly = true;
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
  cmdLine.volId = argv[optind];

  // Move on to the next command-line argument
  optind++;

  // Parse the DRIVE command-line argument
  cmdLine.driveId = argv[optind];

  return cmdLine;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::rmc::MountCmd::usage(std::ostream &os)
  const throw() {
  os <<
  "Usage:\n"
  "\n"
  "  castor-tape-mount [options] VID DRIVE\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID   The indentifier of the volume to be mounted.\n"
  "\n"
  "  DRIVE The drive in one of the following three forms corresponding to the\n"
  "        three supported drive-loader types, namely acs, manual and smc:\n"
  "\n"
  "             smc@rmc_host,drive_ordinal\n"
  "\n"
  "             manual\n"
  "\n"
  "             acs@rmc_host,ACS,LSM,panel,transport\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help             Print this help message and exit.\n"
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
