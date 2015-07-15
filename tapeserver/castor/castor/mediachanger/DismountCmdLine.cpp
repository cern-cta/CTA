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

#include "castor/exception/Exception.hpp"
#include "castor/exception/MissingOperand.hpp"
#include "castor/mediachanger/DismountCmdLine.hpp"
#include "castor/mediachanger/LibrarySlotParser.hpp"

#include <getopt.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::mediachanger::DismountCmdLine::DismountCmdLine() throw():
  m_debug(false),
  m_help(false),
  m_driveLibrarySlot(0),
  m_force(false) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::mediachanger::DismountCmdLine::DismountCmdLine(const int argc,
  char *const *const argv):
  m_debug(false),
  m_help(false),
  m_driveLibrarySlot(0),
  m_force(false) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"force", 0, NULL, 'f'},
    {NULL   , 0, NULL, 0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":dhf", longopts, NULL)) != -1) {
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
    castor::exception::MissingOperand ex;
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
castor::mediachanger::DismountCmdLine::DismountCmdLine(
  const DismountCmdLine &obj):
  m_debug(obj.m_debug),
  m_help(obj.m_help),
  m_vid(obj.m_vid),
  m_driveLibrarySlot(0 == obj.m_driveLibrarySlot ? 0 :
    obj.m_driveLibrarySlot->clone()),
  m_force(obj.m_force) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::mediachanger::DismountCmdLine::~DismountCmdLine() throw() {
  delete m_driveLibrarySlot;
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
castor::mediachanger::DismountCmdLine &castor::mediachanger::DismountCmdLine::
  operator=(const DismountCmdLine &rhs) {
  // If this is not a self assigment
  if(this != &rhs) {
    // Avoid a memory leak
    delete(m_driveLibrarySlot);

    m_debug = rhs.m_debug;
    m_help  = rhs.m_help;
    m_vid   = rhs.m_vid;
    m_driveLibrarySlot = 0 == rhs.m_driveLibrarySlot ? 0 :
      rhs.m_driveLibrarySlot->clone();
    m_force = rhs.m_force;
  }

  return *this;
}

//------------------------------------------------------------------------------
// getForce
//------------------------------------------------------------------------------
bool castor::mediachanger::DismountCmdLine::getForce() const throw() {
  return m_force;
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void castor::mediachanger::DismountCmdLine::processOption(const int opt) {
  switch(opt) {
  case 'd':
    m_debug = true;
    break;
  case 'h':
    m_help = true;
    break;
  case 'f':
    m_force = true;
    break;
  case ':':
    return handleMissingParameter(optopt);
  case '?':
    return handleUnknownOption(optopt);
  default:
    {
      castor::exception::Exception ex;
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
std::string castor::mediachanger::DismountCmdLine::getUsage() throw() {
  return
  "Usage:\n"
  "\n"
  "  castor-tape-mediachanger-dismount [options] VID DRIVE_SLOT\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID        The VID of the volume to be dismounted.\n"
  "  DRIVE_SLOT The slot in the tape library where the drive is located.\n"
  "             DRIVE_SLOT must be in one of the following two forms:\n"
  "\n"
  "             acsACS_NUMBER,LSM_NUMBER,PANEL_NUMBER,TRANSPORT_NUMBER\n"
  "             smc@rmc_host,drive_ordinal\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help  Print this help message and exit.\n"
  "\n"
  "  -f|--force Force the dismount (rewind and eject the tape where necessary)."
    "\n"
  "Comments to: Castor.Support@cern.ch\n";
}

//------------------------------------------------------------------------------
// getDebug
//------------------------------------------------------------------------------
bool castor::mediachanger::DismountCmdLine::getDebug() const throw() {
  return m_debug;
}

//------------------------------------------------------------------------------
// getHelp
//------------------------------------------------------------------------------
bool castor::mediachanger::DismountCmdLine::getHelp() const throw() {
  return m_help;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &castor::mediachanger::DismountCmdLine::getVid()
  const throw() {
  return m_vid;
}

//------------------------------------------------------------------------------
// getDriveLibrarySlot
//------------------------------------------------------------------------------
const castor::mediachanger::LibrarySlot &castor::mediachanger::
  DismountCmdLine::getDriveLibrarySlot() const {
  if(0 == m_driveLibrarySlot) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to get drive library-slot: Value not set";
    throw ex;
  }

  return *m_driveLibrarySlot;
}

