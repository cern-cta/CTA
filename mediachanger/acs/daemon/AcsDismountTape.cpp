/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
 
#include "AcsDismountTape.hpp"
#include "common/exception/DismountFailed.hpp"
#include "common/log/log.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
//cta::mediachanger::acs::daemon::acs::daemon::AcsDismountTape::AcsDismountTape(
cta::mediachanger::acs::daemon::AcsDismountTape::AcsDismountTape(
  const std::string &vid,
  const uint32_t acs,
  const uint32_t lsm,
  const uint32_t panel,
  const uint32_t drive,
  Acs &acsWrapper,
  log::Logger& log,
  const AcsdConfiguration &ctaConf):
  cta::mediachanger::acs::AcsLibraryInteraction(acsWrapper, log),
  m_volId(acsWrapper.str2Volid(vid)),
  m_driveId(acsWrapper.alpd2DriveId(acs,lsm,panel,drive)), 
  m_acsWrapper(acsWrapper),
  m_log(log),
  m_ctaConf(ctaConf) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsDismountTape::execute() const {
//void cta::mediachanger::acs::daemon::AcsDismountTape::execute() const {
  syncDismount();
}

//------------------------------------------------------------------------------
// asyncExecute
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsDismountTape::asyncExecute(const SEQ_NO seqNo) const {
//void cta::mediachanger::acs::daemon::AcsDismountTape::asyncExecute(const SEQ_NO seqNo) const {
  asyncDismount(seqNo);
}


//------------------------------------------------------------------------------
// syncDismount
//------------------------------------------------------------------------------
//void cta::mediachanger::acs::daemon::AcsDismountTape::syncDismount() const {
void cta::mediachanger::acs::daemon::AcsDismountTape::syncDismount() const {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendDismountRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf,
     m_ctaConf.QueryInterval.value(),
     m_ctaConf.CmdTimeout.value());
    processDismountResponse(buf);
  } catch(cta::exception::Exception &ex) {
    cta::exception::DismountFailed df;
    df.getMessage() << "Failed to dismount volume " <<
      m_volId.external_label << ": " << ex.getMessage().str();     
    throw df;
  }
}

//------------------------------------------------------------------------------
// asyncDismount
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsDismountTape::asyncDismount(const SEQ_NO seqNo) const
  {
  try {
    sendDismountRequest(seqNo);    
  } catch(cta::exception::Exception &ex) {
    cta::exception::DismountFailed df;
    df.getMessage() << "Failed to send dismount request to ACS " <<
      m_volId.external_label << ": " << ex.getMessage().str();     
    throw df;
  }
}

//------------------------------------------------------------------------------
// sendDismountRequest
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsDismountTape::sendDismountRequest(
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
    cta::exception::DismountFailed ex;
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
void cta::mediachanger::acs::daemon::AcsDismountTape::processDismountResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
  {
  const ACS_DISMOUNT_RESPONSE *const msg = (ACS_DISMOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->dismount_status) {
    cta::exception::DismountFailed ex;
    ex.getMessage() << "Status of dismount response is not success: " <<
      acs_status(msg->dismount_status);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsDismountTape::~AcsDismountTape() throw() {  
}
