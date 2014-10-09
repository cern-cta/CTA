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
 
#include "castor/acs/AcsMountTapeReadOnly.hpp"
#include "castor/exception/MountFailed.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::acs::AcsMountTapeReadOnly::AcsMountTapeReadOnly(
  const std::string &vid,
  const uint32_t acs,
  const uint32_t lsm,
  const uint32_t panel,
  const uint32_t drive,
  Acs &acsWrapper,
  log::Logger &log,
  const AcsDaemon::CastorConf &castorConf):
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
void castor::acs::AcsMountTapeReadOnly::execute() const {
  syncMountTapeReadOnly();
}

//------------------------------------------------------------------------------
// syncMountTapeReadOnly
//------------------------------------------------------------------------------
void castor::acs::AcsMountTapeReadOnly::syncMountTapeReadOnly() const
  {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendMountTapeReadOnlyRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, 
      m_castorConf.acsQueryLibraryInterval, m_castorConf.acsCommandTimeout);
    processMountTapeReadOnlyResponse(buf);
  }  catch(castor::exception::Exception &ex) {
    castor::exception::MountFailed mf;
    mf.getMessage() << "Failed to mount for read only access volume " <<
      m_volId.external_label << ": " << ex.getMessage().str();
    throw mf;
  }
}

//------------------------------------------------------------------------------
// sendMountTapeReadOnlyRequest
//------------------------------------------------------------------------------
void castor::acs::AcsMountTapeReadOnly::sendMountTapeReadOnlyRequest(
  const SEQ_NO seqNumber) const {
  const LOCKID lockId    = 0; // No lock
  const BOOLEAN bypass   = FALSE;
  const BOOLEAN readOnly = TRUE;

  m_log(LOG_DEBUG,"Calling Acs::mount()");
  const STATUS s = m_acsWrapper.mount(seqNumber, lockId, m_volId,
    m_driveId, readOnly, bypass);
  std::stringstream dbgMsg;
  dbgMsg << "Acs::mount() returned " << acs_status(s);            
  m_log(LOG_DEBUG,dbgMsg.str());

  if(STATUS_SUCCESS != s) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Failed to send request to mount for read only access"
      " volume " << m_volId.external_label << " into drive " <<
      m_acsWrapper.driveId2Str(m_driveId) << ": readOnly=" <<
      (readOnly ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  } 
}

//------------------------------------------------------------------------------
// processMountTapeReadOnlyResponse
//------------------------------------------------------------------------------
void castor::acs::AcsMountTapeReadOnly::processMountTapeReadOnlyResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
  {
  const ACS_MOUNT_RESPONSE *const msg = (ACS_MOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->mount_status) {
    castor::exception::MountFailed ex;
    ex.getMessage() << "Status of mount for read only access response is not"
      " success: " << acs_status(msg->mount_status);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::acs::AcsMountTapeReadOnly::~AcsMountTapeReadOnly() throw() {  
}
