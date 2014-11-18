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

#include "castor/acs/AcsDismountCmd.hpp"

#include <getopt.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsDismountCmd::AcsDismountCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs), m_defaultQueryInterval(10),
  m_defaultTimeout(600) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsDismountCmd::~AcsDismountCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::acs::AcsDismountCmd::main(const int argc, char *const *const argv)
  throw() {
  try {
    m_cmdLine = AcsDismountCmdLine(argc, argv, m_defaultQueryInterval,
      m_defaultTimeout);
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
void castor::acs::AcsDismountCmd::usage(std::ostream &os) const throw() {
  os <<
  "Usage:\n"
  "\n"
  "  castor-tape-acs-dismount [options] VID DRIVE\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be dismounted.\n"
  "\n"
  "  DRIVE  The drive from which the volume is to be dismounted.\n"
  "         The format of DRIVE is:\n"
  "\n"
  "             ACS:LSM:panel:transport\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "\n"
  "  -f|--force            Force the dismount.\n"
  "\n"
  "  -h|--help             Print this help message and exit.\n"
  "\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << m_defaultQueryInterval << ".\n"
  "\n"
  "  -t|--timeout SECONDS  Time to wait for the dismount to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS is "
    << m_defaultTimeout << ".\n"
  "\n"
  "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// syncDismount
//------------------------------------------------------------------------------
void castor::acs::AcsDismountCmd::syncDismount() {
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
void castor::acs::AcsDismountCmd::sendDismountRequest(
  const SEQ_NO seqNumber) {
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
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processDismountResponse
//------------------------------------------------------------------------------
void castor::acs::AcsDismountCmd::processDismountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) {
  const ACS_DISMOUNT_RESPONSE *const msg = (ACS_DISMOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->dismount_status) {
    castor::exception::DismountFailed ex;
    ex.getMessage() << "Status of dismount response is not success: " <<
      acs_status(msg->dismount_status);
    throw ex;
  }
}
