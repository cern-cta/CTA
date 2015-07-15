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

#include "castor/acs/AcsQueryVolumeCmd.hpp"
#include "castor/acs/AcsQueryVolumeCmdLine.hpp"

#include <getopt.h>
#include <iostream>
#include <string.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsQueryVolumeCmd::AcsQueryVolumeCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsQueryVolumeCmd::~AcsQueryVolumeCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int castor::acs::AcsQueryVolumeCmd::exceptionThrowingMain(const int argc,
  char *const *const argv) {
  try {
    m_cmdLine = AcsQueryVolumeCmdLine(argc, argv);
  } catch(castor::exception::Exception &ex) {
    m_err << ex.getMessage().str() << std::endl;
    m_err << std::endl;
    m_err << m_cmdLine.getUsage() << std::endl;
    return 1;
  }

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    m_out << AcsQueryVolumeCmdLine::getUsage();
    return 0;
  }

  // Setup debug mode to be on or off depending on the command-line arguments
  m_debugBuf.setDebug(m_cmdLine.debug);

  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;

  syncQueryVolume();
  return 0;
}

//------------------------------------------------------------------------------
// syncQueryVolume
//------------------------------------------------------------------------------
void castor::acs::AcsQueryVolumeCmd::syncQueryVolume() {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendQueryVolumeRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, m_cmdLine.queryInterval,
      m_cmdLine.timeout);
    processQueryResponse(m_out, buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::QueryVolumeFailed qf;
    qf.getMessage() << "Failed to query volume " <<
      m_cmdLine.volId.external_label << ": " << ex.getMessage().str();
    throw qf;
  }
}

//------------------------------------------------------------------------------
// sendQueryVolumeRequest
//------------------------------------------------------------------------------
void castor::acs::AcsQueryVolumeCmd::sendQueryVolumeRequest(
  const SEQ_NO seqNumber)  {
  VOLID volIds[MAX_ID];

  memset(volIds, '\0', sizeof(volIds));
  strncpy(volIds[0].external_label, m_cmdLine.volId.external_label,
    sizeof(volIds[0].external_label));
  volIds[0].external_label[sizeof(volIds[0].external_label) - 1]  = '\0';

  m_dbg << "Calling Acs::queryVolume()" << std::endl;
  const STATUS s = m_acs.queryVolume(seqNumber, volIds, 1);
  m_dbg << "Acs::queryVolume() returned " << acs_status(s) << std::endl;

  if(STATUS_SUCCESS != s) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to send query request for volume " <<
      m_cmdLine.volId.external_label << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processQueryResponse
//------------------------------------------------------------------------------
void castor::acs::AcsQueryVolumeCmd::processQueryResponse(
  std::ostream &os,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) {

  const ACS_QUERY_VOL_RESPONSE *const msg = (ACS_QUERY_VOL_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->query_vol_status) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Status of query response is not success: " <<
      acs_status(msg->query_vol_status);
    throw ex;
  }

  if((unsigned short)1 != msg->count) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Query response does not contain a single volume: count="
      << msg->count;
    throw ex;
  }

  // count is 1 so it is safe to make a reference to the single volume status
  const QU_VOL_STATUS &volStatus = msg->vol_status[0];

  if(strcmp(m_cmdLine.volId.external_label, volStatus.vol_id.external_label)) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() <<
      "Volume identifier of query response does not match that of request"
      ": requestVID=" << m_cmdLine.volId.external_label <<
      " responseVID=" << volStatus.vol_id.external_label;
    throw ex;
  }

  writeVolumeStatus(os, volStatus);
}

//------------------------------------------------------------------------------
// writeVolumeStatus
//------------------------------------------------------------------------------
void castor::acs::AcsQueryVolumeCmd::writeVolumeStatus(
  std::ostream &os, const QU_VOL_STATUS &s) throw() {
  os << "Volume identifier: " << s.vol_id.external_label << std::endl;
  os << "Media type (media_types.dat): " << (int)s.media_type << std::endl;

  switch(s.location_type) {
  case LOCATION_CELL: {
    os << "Location type: cell" << std::endl;
    const CELLID &cellId = s.location.cell_id;
    os << "ACS: " << (int)cellId.panel_id.lsm_id.acs << std::endl;
    os << "LSM: " << (int)cellId.panel_id.lsm_id.lsm << std::endl;
    os << "Panel: " << (int)cellId.panel_id.panel << std::endl;
    os << "Row: " << (int)cellId.row << std::endl;
    os << "Column: " << (int)cellId.col << std::endl;
    break;
  }
  case LOCATION_DRIVE: {
    os << "Location type: drive" << std::endl;
    const DRIVEID &driveId = s.location.drive_id;
    os << "ACS: " << (int)driveId.panel_id.lsm_id.acs << std::endl;
    os << "LSM: " << (int)driveId.panel_id.lsm_id.lsm << std::endl;
    os << "Panel: " << (int)driveId.panel_id.panel << std::endl;
    os << "Drive: " << (int)driveId.drive << std::endl;
    break;
  }
  default:
    os << "Location type: UNKNOWN" << std::endl;
    break;
  }

  os << "Status: " << acs_status(s.status) << std::endl;
}
