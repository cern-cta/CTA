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
 
#include "AcsMountTapeReadWrite.hpp"
#include "common/exception/MountFailed.hpp"
#include "mediachanger/acs/AcsLibraryInteraction.hpp"
#include "common/exception/QueryVolumeFailed.hpp"
#include "common/log/log.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::AcsMountTapeReadWrite(
  const std::string &vid,
  const uint32_t acs,
  const uint32_t lsm,
  const uint32_t panel,
  const uint32_t drive,
  cta::mediachanger::acs::Acs &acsWrapper,
  cta::log::Logger &log,
  const AcsdConfiguration &ctaConf):
  AcsLibraryInteraction(acsWrapper, log),
  m_volId(acsWrapper.str2Volid(vid)),
  m_driveId(acsWrapper.alpd2DriveId(acs,lsm,panel,drive)), 
  m_acsWrapper(acsWrapper),
  m_log(log),
  m_ctaConf(ctaConf) {
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::execute() const {
  try {
    syncMountTapeReadWrite();
  } catch (cta::exception::MountFailed &mountFailed) {
    try {
      const std::string queryVolumeResponse = syncQueryVolume();
      mountFailed.getMessage() << " : The query volume response: " << 
        queryVolumeResponse;
    } catch (cta::exception::QueryVolumeFailed &queryFailed) {
      mountFailed.getMessage() << " : " << queryFailed.getMessage().str();
    }
    throw mountFailed;
  }  
}

//------------------------------------------------------------------------------
// syncMountTapeReadWrite
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::syncMountTapeReadWrite() const
  {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  try {
    sendMountTapeReadWriteRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, 
      m_ctaConf.QueryInterval.value(), m_ctaConf.CmdTimeout.value());
    processMountTapeReadWriteResponse(buf);
  }  catch(cta::exception::Exception &ex) {
    cta::exception::MountFailed mf;
    mf.getMessage() << "Failed to mount for read/write access volume " <<
      m_volId.external_label << ": " << ex.getMessage().str();
    throw mf;
  }
}

//------------------------------------------------------------------------------
// sendMountTapeReadWriteRequest
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::sendMountTapeReadWriteRequest(
  const SEQ_NO seqNumber) const {
  const LOCKID lockId    = 0; // No lock
  const BOOLEAN bypass   = FALSE;
  const BOOLEAN readOnly = FALSE;

  m_log(LOG_DEBUG,"Calling Acs::mount()");
  const STATUS s = m_acsWrapper.mount(seqNumber, lockId, m_volId,
    m_driveId, readOnly, bypass);
  std::stringstream dbgMsg;
  dbgMsg << "Acs::mount() returned " << acs_status(s);          
  m_log(LOG_DEBUG,dbgMsg.str());

  if(STATUS_SUCCESS != s) {
    cta::exception::MountFailed ex;
    ex.getMessage() << "Failed to send request to mount for read/write access"
      " volume " << m_volId.external_label << " into drive " 
      << m_acsWrapper.driveId2Str(m_driveId) 
      << ": readOnly=" 
      << (readOnly ? "TRUE" : "FALSE") << ": " << acs_status(s);
    throw ex;
  } 
}

//------------------------------------------------------------------------------
// processMountTapeReadWriteResponse
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::processMountTapeReadWriteResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const
  {
  const ACS_MOUNT_RESPONSE *const msg = (ACS_MOUNT_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->mount_status) {
    cta::exception::MountFailed ex;
    ex.getMessage() << "Status of mount response is not success: " << 
      acs_status(msg->mount_status);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// syncQueryVolume
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::syncQueryVolume() const {
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];
  try {
    sendQueryVolumeRequest(requestSeqNumber);
    requestResponsesUntilFinal(requestSeqNumber, buf, 
      m_ctaConf.QueryInterval.value(), m_ctaConf.CmdTimeout.value());
    return processQueryResponse(buf);
  } catch(cta::exception::Exception &ex) {
    cta::exception::QueryVolumeFailed qf;
    qf.getMessage() << "Failed to query volume " <<
      m_volId.external_label << ": " << ex.getMessage().str();
    throw qf;
  }
}

//------------------------------------------------------------------------------
// sendQueryVolumeRequest
//------------------------------------------------------------------------------
void cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::sendQueryVolumeRequest (
  const SEQ_NO seqNumber) const {
  VOLID volIds[MAX_ID];

  memset(volIds, '\0', sizeof(volIds));
  strncpy(volIds[0].external_label, m_volId.external_label,
    sizeof(volIds[0].external_label));
  volIds[0].external_label[sizeof(volIds[0].external_label) - 1]  = '\0';
           
  m_log(LOG_DEBUG,"Calling Acs::queryVolume()");
  
  
  const STATUS s = m_acs.queryVolume(seqNumber, volIds, 1);

  std::stringstream dbgMsg;
  dbgMsg << "Acs::queryVolume() returned " << acs_status(s);            
  m_log(LOG_DEBUG,"Calling Acs::queryVolume()");

  if(STATUS_SUCCESS != s) {
    cta::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to send query request for volume " <<
      m_volId.external_label << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processQueryResponse
//------------------------------------------------------------------------------
std::string cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::processQueryResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)]) const {

  const ACS_QUERY_VOL_RESPONSE *const msg = (ACS_QUERY_VOL_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->query_vol_status) {
    cta::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Status of query response is not success: " <<
      acs_status(msg->query_vol_status);
    throw ex;
  }

  if((unsigned short)1 != msg->count) {
    cta::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Query response does not contain a single volume: count="
      << msg->count;
    throw ex;
  }

  // count is 1 so it is safe to make a reference to the single volume status
  const QU_VOL_STATUS &volStatus = msg->vol_status[0];

  if(strcmp(m_volId.external_label, volStatus.vol_id.external_label)) {
    cta::exception::QueryVolumeFailed ex;
    ex.getMessage() <<
      "Volume identifier of query response does not match that of request"
      ": requestVID=" << m_volId.external_label <<
      " responseVID=" << volStatus.vol_id.external_label;
    throw ex;
  }

  return volumeStatusAsString(volStatus);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::acs::daemon::AcsMountTapeReadWrite::~AcsMountTapeReadWrite() {  
}
