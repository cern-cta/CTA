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

#include "castor/acs/AcsMountCmd.hpp"
#include "castor/acs/AcsMountCmdLine.hpp"

#include <getopt.h>
#include <iostream>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsMountCmd::AcsMountCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsMountCmd::~AcsMountCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
void castor::acs::AcsMountCmd::exceptionThrowingMain(const int argc,
  char *const *const argv) {
  m_cmdLine = AcsMountCmdLine(argc, argv);

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    m_out << AcsMountCmdLine::getUsage();
    return;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "readonly = " << bool2Str(m_cmdLine.readOnly) << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;
  m_dbg << "DRIVE_SLOT = " << m_acs.driveId2Str(m_cmdLine.libraryDriveSlot)
    << std::endl;

  syncMount();
}

//------------------------------------------------------------------------------
// syncMount
//------------------------------------------------------------------------------
void castor::acs::AcsMountCmd::syncMount() {
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
void castor::acs::AcsMountCmd::sendMountRequest(const SEQ_NO seqNumber) {
  const LOCKID lockId = 0; // No lock
  const BOOLEAN bypass = FALSE;

  m_dbg << "Calling Acs::mount()" << std::endl;
  const STATUS s = m_acs.mount(seqNumber, lockId, m_cmdLine.volId,
    m_cmdLine.libraryDriveSlot, m_cmdLine.readOnly, bypass);
  m_dbg << "Acs::mount() returned " << acs_status(s) << std::endl;

  if(STATUS_SUCCESS != s) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Failed to send request to mount volume " <<
      m_cmdLine.volId.external_label << " into drive " <<
      m_acs.driveId2Str(m_cmdLine.libraryDriveSlot) << ": readOnly=" <<
      (m_cmdLine.readOnly ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processMountResponse
//------------------------------------------------------------------------------
void castor::acs::AcsMountCmd::processMountResponse(
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
