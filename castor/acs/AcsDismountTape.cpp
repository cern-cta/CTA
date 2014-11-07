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
 
#include "castor/acs/AcsDismountTape.hpp"
#include "castor/exception/DismountFailed.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsDismountTape::AcsDismountTape(
  const std::string &vid,
  const uint32_t acs,
  const uint32_t lsm,
  const uint32_t panel,
  const uint32_t drive,
  Acs &acsWrapper,
  log::Logger &log,
  const AcsDaemonConfig &castorConf):
  AcsLibraryInteraction(acsWrapper,log),
  m_volId(acsWrapper.str2Volid(vid)),
  m_driveId(acsWrapper.alpd2DriveId(acs,lsm,panel,drive)), 
  m_acsWrapper(acsWrapper),
  m_log(log),
  m_castorConf(castorConf) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::execute() const {
  syncDismount();
}

//------------------------------------------------------------------------------
// asyncExecute
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::asyncExecute(const SEQ_NO seqNo) const {
  asyncDismount(seqNo);
}


//------------------------------------------------------------------------------
// syncDismount
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::syncDismount() const {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendDismountRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf,
      m_castorConf.queryInterval,
      m_castorConf.cmdTimeout);
    processDismountResponse(buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::DismountFailed df;
    df.getMessage() << "Failed to dismount volume " <<
      m_volId.external_label << ": " << ex.getMessage().str();     
    throw df;
  }
}

//------------------------------------------------------------------------------
// asyncDismount
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::asyncDismount(const SEQ_NO seqNo) const
  {
  try {
    sendDismountRequest(seqNo);    
  } catch(castor::exception::Exception &ex) {
    castor::exception::DismountFailed df;
    df.getMessage() << "Failed to send dismount request to ACS " <<
      m_volId.external_label << ": " << ex.getMessage().str();     
    throw df;
  }
}

//------------------------------------------------------------------------------
// sendDismountRequest
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::sendDismountRequest(
  const SEQ_NO seqNumber) const {
  const LOCKID lockId = 0; // No lock
  const BOOLEAN force = FALSE; 
  
  std::stringstream dbgMsg;
  dbgMsg << "Calling Acs::dismount() with seqNumber=" << seqNumber;
  m_log(LOG_DEBUG, dbgMsg.str());
  const STATUS s = m_acsWrapper.dismount(seqNumber, lockId, m_volId,
    m_driveId, force);
  
  dbgMsg.str("");
  dbgMsg << "Acs::dismount() for seqNumber=" << seqNumber << " returned " <<
    acs_status(s);           
  m_log(LOG_DEBUG,dbgMsg.str());
  if(STATUS_SUCCESS != s) {
    castor::exception::DismountFailed ex;
    ex.getMessage() << "Failed to send request to dismount volume " <<
      m_volId.external_label << " from drive " <<
      m_acsWrapper.driveId2Str(m_driveId) << ": force=" <<
      (force ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processDismountResponse
//------------------------------------------------------------------------------
void castor::acs::AcsDismountTape::processDismountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
  {
  const ACS_DISMOUNT_RESPONSE *const msg = (ACS_DISMOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->dismount_status) {
    castor::exception::DismountFailed ex;
    ex.getMessage() << "Status of dismount response is not success: " <<
      acs_status(msg->dismount_status);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsDismountTape::~AcsDismountTape() throw() {  
}
