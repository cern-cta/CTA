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


#include "AcsQueryDriveCmd.hpp"
#include "AcsQueryDriveCmdLine.hpp"
#include <getopt.h>
#include <iostream>
#include <string.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsQueryDriveCmd::AcsQueryDriveCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs):
  AcsCmd(inStream, outStream, errStream, acs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::AcsQueryDriveCmd::~AcsQueryDriveCmd() {
  // Do nothing
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int cta::mediachanger::acs::AcsQueryDriveCmd::exceptionThrowingMain(const int argc,
  char *const *const argv) {
  try {
    m_cmdLine = AcsQueryDriveCmdLine(argc, argv);
  } catch(cta::exception::Exception &ex) {
    m_err << ex.getMessage().str() << std::endl;
    m_err << std::endl;
    m_err << m_cmdLine.getUsage() << std::endl;
    return 1;
  }

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    m_out << AcsQueryDriveCmdLine::getUsage();
    return 0;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "DRIVE_SLOT = " << m_acs.driveId2Str(m_cmdLine.libraryDriveSlot) << std::endl;

  syncQueryDrive();
  return 0;
}

//------------------------------------------------------------------------------
// syncQueryDrive
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsQueryDriveCmd::syncQueryDrive() {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendQueryDriveRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, m_cmdLine.queryInterval,
      m_cmdLine.timeout);
    processQueryResponse(m_out, buf);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to query drive " <<
      m_acs.driveId2Str(m_cmdLine.libraryDriveSlot) << ": " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// sendQueryDriveRequest
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsQueryDriveCmd::sendQueryDriveRequest(
  const SEQ_NO sequence)  {

  m_dbg << "Calling Acs::queryDrive()" << std::endl;
  const STATUS s = acs_query_drive(sequence, &(m_cmdLine.libraryDriveSlot), 1);
  m_dbg << "Acs::queryDrive() returned " << acs_status(s) << std::endl;

  if(STATUS_SUCCESS != s) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to send query request for drive " <<
     m_acs.driveId2Str(m_cmdLine.libraryDriveSlot)<< ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processQueryResponse
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsQueryDriveCmd::processQueryResponse(
  std::ostream &os,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) {

  const ACS_QUERY_DRV_RESPONSE *const msg = (ACS_QUERY_DRV_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->query_drv_status) {
    cta::exception::Exception ex;
    ex.getMessage() << "Status of query response is not success: " <<
      acs_status(msg->query_drv_status);
    throw ex;
  }

  if((unsigned short)1 != msg->count) {
    cta::exception::Exception ex;
    ex.getMessage() << "Query response does not contain a single drive: count="
      << msg->count;
    throw ex;
  }

  // count is 1 so it is safe to make a reference to the single drive status
  const QU_DRV_STATUS &drvStatus = msg->drv_status[0];

  if(m_acs.driveId2Str(m_cmdLine.libraryDriveSlot)!= m_acs.driveId2Str(drvStatus.drive_id)) {
    cta::exception::Exception ex;
    ex.getMessage() <<
      "Drive identifier of query response does not match that of request"
      " requestDriveID=" <<m_acs.driveId2Str(m_cmdLine.libraryDriveSlot) <<
      " responseDriveID=" << m_acs.driveId2Str(drvStatus.drive_id);
    throw ex;
  }

  writeDriveStatus(os, drvStatus);
}

//------------------------------------------------------------------------------
// writeDriveStatus
//------------------------------------------------------------------------------
void cta::mediachanger::acs::AcsQueryDriveCmd::writeDriveStatus(
  std::ostream &os, const QU_DRV_STATUS &s) {

  os << "Drive identifier: " << m_acs.driveId2Str(s.drive_id) << std::endl;
  os << "Drive type: " << acs_type((TYPE)s.drive_type) << std::endl;
  os << "Drive state: " << acs_state(s.state) << std::endl;
  os << "Drive status: " << acs_status(s.status) << std::endl;
  os << "Volume identifier: " << s.vol_id.external_label << std::endl;
}
