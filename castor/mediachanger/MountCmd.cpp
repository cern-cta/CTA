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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/mediachanger/MountCmd.hpp"

#include <getopt.h>
#include <iostream>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::MountCmd::MountCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  MediaChangerFacade &mc) throw():
  CmdLineTool(inStream, outStream, errStream, mc) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::mediachanger::MountCmd::~MountCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::mediachanger::MountCmd::main(const int argc,
  char *const *const argv) throw() {
  if(parseCmdLine(argc, argv)) {
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

  m_dbg << "readonly   = " << bool2Str(m_cmdLine.readOnly) << std::endl;
  m_dbg << "VID        = " << m_cmdLine.vid;
  m_dbg << "DRIVE_SLOT = " << m_cmdLine.driveLibrarySlot.str() << std::endl;

  return mountTape();
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
bool castor::mediachanger::MountCmd::parseCmdLine(const int argc,
  char *const *const argv) throw() {
  std::ostringstream errorMessage;

  try {
    m_cmdLine = MountCmdLine(argc, argv);
    return false; // Successfully parsed command line
  } catch(castor::exception::InvalidArgument &ia) {
    errorMessage << "Invalid argument: " << ia.getMessage().str() << std::endl;
    errorMessage << std::endl;
    usage(errorMessage);
  } catch(castor::exception::MissingOperand &mo) {
    errorMessage << "Missing operand: " << mo.getMessage().str() << std::endl;
    errorMessage << std::endl;
    usage(errorMessage);
  } catch(castor::exception::Exception &ie) {
    errorMessage << "Internal error: " << ie.getMessage().str() << std::endl;
  } catch(std::exception &se) {
    errorMessage << "Unexpected exception: " << se.what() << std::endl;
  } catch(...) {
    errorMessage << "Unknown exception" << std::endl;
  }

  // Reaching here means the command line could not be parsed, an exception has
  // been thrown and errorMessage has been set accordingly

  m_err << "Aborting: Failed to parse command line: " << errorMessage.str();

  return true; // Failed to parse command line
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::mediachanger::MountCmd::usage(std::ostream &os) const throw() {
  os <<
  "Usage:\n"
  "\n"
  "  castor-tape-mediachanger-mount [options] VID DRIVE_SLOT\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID        The VID of the volume to be mounted.\n"
  "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
  "             DRIVE_SLOT must be in one of the following three forms:\n"
  "\n"
  "             acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER\n"
  "             manual\n"
  "             smc@rmc_host,drive_ordinal\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug    Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help     Print this help message and exit.\n"
  "\n"
  "  -r|--readOnly Request the volume is mounted for read-only access\n"
  "\n"
  "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// mountTape
//------------------------------------------------------------------------------
int castor::mediachanger::MountCmd::mountTape() throw() {
  try {
    if(m_cmdLine.readOnly) {
      m_mc.mountTapeReadOnly(m_cmdLine.vid, m_cmdLine.driveLibrarySlot);
    } else {
      m_mc.mountTapeReadWrite(m_cmdLine.vid, m_cmdLine.driveLibrarySlot);
    }
  } catch(castor::exception::Exception &ex) {
    m_err << "Aborting: " << ex.getMessage().str() << std::endl;
    return 1;
  }

  return 0;
}
