/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "AcsDismountCmd.hpp"

#include <getopt.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsDismountCmd::AcsDismountCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs):
  AcsCmd(inStream, outStream, errStream, acs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsDismountCmd::~AcsDismountCmd() {
  // Do nothing
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int cta::mediachanger::acs::AcsDismountCmd::exceptionThrowingMain(const int argc,
  char *const *const argv) {
  try {
    m_cmdLine = AcsDismountCmdLine(argc, argv);
  } catch(cta::exception::Exception &ex) {
    m_err << ex.getMessage().str() << std::endl;
    m_err << std::endl;
    m_err << m_cmdLine.getUsage() << std::endl;
    return 1;
  }

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    m_out << AcsDismountCmdLine::getUsage();
    return 0;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "force = " << (m_cmdLine.force ? "TRUE" : "FALSE") << std::endl;
  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;
  m_dbg << "DRIVE_SLOT = " << m_acs.driveId2Str(m_cmdLine.libraryDriveSlot)
    << std::endl;

  syncDismount();
  return 0;
}

//------------------------------------------------------------------------------
// syncDismount
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsDismountCmd::syncDismount() {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendDismountRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, m_cmdLine.queryInterval,
      m_cmdLine.timeout);
    processDismountResponse(buf);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to dismount volume " <<
      m_cmdLine.volId.external_label << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// sendDismountRequest
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsDismountCmd::sendDismountRequest(
  const SEQ_NO seqNumber) {
  const LOCKID lockId = 0; // No lock

  m_dbg << "Calling Acs::dismount()" << std::endl;
  const STATUS s = m_acs.dismount(seqNumber, lockId, m_cmdLine.volId,
    m_cmdLine.libraryDriveSlot, m_cmdLine.force);
  m_dbg << "Acs::dismount() returned " << acs_status(s) << std::endl;
  if(STATUS_SUCCESS != s) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to send request to dismount volume " <<
      m_cmdLine.volId.external_label << " from drive " <<
      m_acs.driveId2Str(m_cmdLine.libraryDriveSlot) << ": force=" <<
      (m_cmdLine.force ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processDismountResponse
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsDismountCmd::processDismountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) {
  const ACS_DISMOUNT_RESPONSE *const msg = (ACS_DISMOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->dismount_status) {
    cta::exception::Exception ex;
    ex.getMessage() << "Status of dismount response is not success: " <<
      acs_status(msg->dismount_status);
    throw ex;
  }
}
