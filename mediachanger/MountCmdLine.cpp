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

#include "common/exception/MissingOperand.hpp"
#include "mediachanger/MountCmdLine.hpp"
#include "common/exception/Exception.hpp"

#include <getopt.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::MountCmdLine::MountCmdLine() throw():
  m_debug(false),
  m_help(false),
  m_readOnly(false),
  m_driveLibrarySlot(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::MountCmdLine::MountCmdLine(const int argc,
  char *const *const argv):
  m_debug(false),
  m_help(false),
  m_readOnly(false),
  m_driveLibrarySlot(0) {

  static struct option longopts[] = {
    {"debug"    , 0, NULL, 'd'},
    {"help"     , 0, NULL, 'h'},
    {"readonly" , 0, NULL, 'r'},
    {NULL       , 0, NULL, 0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":dhr", longopts, NULL)) != -1) {
    processOption(opt);
  }

  // There is no need to continue parsing when the help option is set
  if(m_help) {
    return;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  // Check that both VID and DRIVE_SLOT has been specified
  if(nbArgs < 2) {
    cta::exception::MissingOperand ex;
    ex.getMessage() << "Both VID and DRIVE_SLOT must be specified";
    throw ex;
  }

  // Parse the VID command-line argument
  m_vid = argv[optind];

  // Move on to the next command-line argument
  optind++;

  // Parse the DRIVE_SLOT command-line argument
  const std::string driveLibrarySlot = argv[optind];
  m_driveLibrarySlot = LibrarySlotParser::parse(driveLibrarySlot);
}

//-----------------------------------------------------------------------------
// copy constructor
//-----------------------------------------------------------------------------
cta::mediachanger::MountCmdLine::MountCmdLine(const MountCmdLine &obj):
  m_debug(obj.m_debug),
  m_help(obj.m_help),
  m_readOnly(obj.m_readOnly),
  m_vid(obj.m_vid),
  m_driveLibrarySlot(0 == obj.m_driveLibrarySlot ? 0 :
    obj.m_driveLibrarySlot->clone()) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
cta::mediachanger::MountCmdLine::~MountCmdLine() throw() {
  delete m_driveLibrarySlot;
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
cta::mediachanger::MountCmdLine &cta::mediachanger::MountCmdLine::
  operator=(const MountCmdLine &rhs) {
  // If this is not a self assigment
  if(this != &rhs) {
    // Avoid a memory leak
    delete(m_driveLibrarySlot);

    m_debug    = rhs.m_debug;
    m_help     = rhs.m_help;
    m_readOnly = rhs.m_readOnly;
    m_vid      = rhs.m_vid;
    m_driveLibrarySlot = 0 == rhs.m_driveLibrarySlot ? 0 :
      rhs.m_driveLibrarySlot->clone();
  }

  return *this;
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void cta::mediachanger::MountCmdLine::processOption(const int opt) {
  switch(opt) {
  case 'd':
    m_debug = true;
    break;
  case 'h':
    m_help = true;
    break;
  case 'r':
    m_readOnly = true;
    break;
  case ':':
    return handleMissingParameter(optopt);
  case '?':
    return handleUnknownOption(optopt);
  default:
    {
      cta::exception::Exception ex;
      ex.getMessage() <<
        "getopt_long returned the following unknown value: 0x" <<
        std::hex << (int)opt;
      throw ex;
    }
  } // switch(opt)
}

//------------------------------------------------------------------------------
// getUsage
//------------------------------------------------------------------------------
std::string cta::mediachanger::MountCmdLine::getUsage() throw() {
  return
  "Usage:\n"
  "\n"
  "  castor-tape-mediachanger-mount [options] VID DRIVE_SLOT\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID        The VID of the volume to be mounted.\n"
  "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
  "             DRIVE_SLOT must be in one of the following two forms:\n"
  "\n"
  "             acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER\n"
  "             smcDRIVE_ORDINAL\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug    Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help     Print this help message and exit.\n"
  "\n"
  "  -r|--readOnly Request the volume is mounted for read-only access\n"
  "\n"
  "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// getDebug
//------------------------------------------------------------------------------
bool cta::mediachanger::MountCmdLine::getDebug() const throw() {
  return m_debug;
}

//------------------------------------------------------------------------------
// getHelp
//------------------------------------------------------------------------------
bool cta::mediachanger::MountCmdLine::getHelp() const throw() {
  return m_help;
}

//------------------------------------------------------------------------------
// getReadOnly
//------------------------------------------------------------------------------
bool cta::mediachanger::MountCmdLine::getReadOnly() const throw() {
  return m_readOnly;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::mediachanger::MountCmdLine::getVid() const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getDriveLibrarySlot
//------------------------------------------------------------------------------
const cta::mediachanger::LibrarySlot &cta::mediachanger::MountCmdLine::
  getDriveLibrarySlot() const {
  if(0 == m_driveLibrarySlot) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get drive library-slot: Value not set";
    throw ex;
  }
  return *m_driveLibrarySlot;
}
