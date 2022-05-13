/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/MissingOperand.hpp"
#include "mediachanger/DismountCmdLine.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "common/exception/Exception.hpp"

#include <getopt.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::mediachanger::DismountCmdLine::DismountCmdLine():
  m_debug(false),
  m_help(false),
  m_driveLibrarySlot(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::DismountCmdLine::DismountCmdLine(const int argc,
  char *const *const argv):
  m_debug(false),
  m_help(false),
  m_driveLibrarySlot(0) {

  static struct option longopts[] = {
    {"debug", 0, nullptr, 'd'},
    {"help" , 0, nullptr, 'h'},
    {nullptr   , 0, nullptr, 0}
  };

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  int opt = 0;
  while((opt = getopt_long(argc, argv, ":dh", longopts, nullptr)) != -1) {
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
cta::mediachanger::DismountCmdLine::DismountCmdLine(
  const DismountCmdLine &obj):
  m_debug(obj.m_debug),
  m_help(obj.m_help),
  m_vid(obj.m_vid),
  m_driveLibrarySlot(0 == obj.m_driveLibrarySlot ? 0 :
    obj.m_driveLibrarySlot->clone()) {
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
cta::mediachanger::DismountCmdLine::~DismountCmdLine() {
  delete m_driveLibrarySlot;
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
cta::mediachanger::DismountCmdLine &cta::mediachanger::DismountCmdLine::
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
  }

  return *this;
}

//------------------------------------------------------------------------------
// getProgramName
//------------------------------------------------------------------------------
std::string cta::mediachanger::DismountCmdLine::getProgramName() {
  return "cta-mediachanger-dismount";
}

//------------------------------------------------------------------------------
// processOption
//------------------------------------------------------------------------------
void cta::mediachanger::DismountCmdLine::processOption(const int opt) {
  switch(opt) {
  case 'd':
    m_debug = true;
    break;
  case 'h':
    m_help = true;
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
std::string cta::mediachanger::DismountCmdLine::getUsage() {
  return std::string() +
  "Usage:\n"
  "\n"
  "  " + getProgramName() + " [options] VID smcDRIVE_ORDINAL\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID              The VID of the volume to be dismounted.\n"
  "  smcDRIVE_ORDINAL The drive ordinal.\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug Turn on the printing of debug information.\n"
  "\n"
  "  -h|--help  Print this help message and exit.\n"
  "\n"
  "Comments to: cta-support@cern.ch\n";
}

//------------------------------------------------------------------------------
// getDebug
//------------------------------------------------------------------------------
bool cta::mediachanger::DismountCmdLine::getDebug() const {
  return m_debug;
}

//------------------------------------------------------------------------------
// getHelp
//------------------------------------------------------------------------------
bool cta::mediachanger::DismountCmdLine::getHelp() const {
  return m_help;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
const std::string &cta::mediachanger::DismountCmdLine::getVid()
  const {
  return m_vid;
}

//------------------------------------------------------------------------------
// getDriveLibrarySlot
//------------------------------------------------------------------------------
const cta::mediachanger::LibrarySlot &cta::mediachanger::
  DismountCmdLine::getDriveLibrarySlot() const {
  if(0 == m_driveLibrarySlot) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get drive library-slot: Value not set";
    throw ex;
  }

  return *m_driveLibrarySlot;
}
