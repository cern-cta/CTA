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
void castor::mediachanger::MountCmd::exceptionThrowingMain(const int argc,
  char *const *const argv) {
  m_cmdLine = MountCmdLine(argc, argv);

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    usage(m_out);
    return;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "readonly   = " << bool2Str(m_cmdLine.readOnly) << std::endl;
  m_dbg << "VID        = " << m_cmdLine.vid << std::endl;
  m_dbg << "DRIVE_SLOT = " << m_cmdLine.driveLibrarySlot.str() << std::endl;

  mountTape();
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
void castor::mediachanger::MountCmd::mountTape() {
  if(m_cmdLine.readOnly) {
    m_mc.mountTapeReadOnly(m_cmdLine.vid, m_cmdLine.driveLibrarySlot);
  } else {
    m_mc.mountTapeReadWrite(m_cmdLine.vid, m_cmdLine.driveLibrarySlot);
  }
}
