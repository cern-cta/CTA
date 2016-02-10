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

#include "common/dataStructures/DriveState.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::DriveState::DriveState() {  
  m_bytesTransferedInSessionSet = false;
  m_currentStateStartTimeSet = false;
  m_currentTapePoolSet = false;
  m_currentVidSet = false;
  m_filesTransferedInSessionSet = false;
  m_latestBandwidthSet = false;
  m_logicalLibrarySet = false;
  m_mountTypeSet = false;
  m_nameSet = false;
  m_sessionIdSet = false;
  m_sessionStartTimeSet = false;
  m_statusSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::DriveState::~DriveState() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::DriveState::allFieldsSet() const {
  return m_bytesTransferedInSessionSet
      && m_currentStateStartTimeSet
      && m_currentTapePoolSet
      && m_currentVidSet
      && m_filesTransferedInSessionSet
      && m_latestBandwidthSet
      && m_logicalLibrarySet
      && m_mountTypeSet
      && m_nameSet
      && m_sessionIdSet
      && m_sessionStartTimeSet
      && m_statusSet;
}

//------------------------------------------------------------------------------
// setBytesTransferedInSession
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setBytesTransferedInSession(const uint64_t bytesTransferedInSession) {
  m_bytesTransferedInSession = bytesTransferedInSession;
  m_bytesTransferedInSessionSet = true;
}

//------------------------------------------------------------------------------
// getBytesTransferedInSession
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::DriveState::getBytesTransferedInSession() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_bytesTransferedInSession;
}

//------------------------------------------------------------------------------
// setCurrentStateStartTime
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setCurrentStateStartTime(const time_t &currentStateStartTime) {
  m_currentStateStartTime = currentStateStartTime;
  m_currentStateStartTimeSet = true;
}

//------------------------------------------------------------------------------
// getCurrentStateStartTime
//------------------------------------------------------------------------------
time_t cta::dataStructures::DriveState::getCurrentStateStartTime() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_currentStateStartTime;
}

//------------------------------------------------------------------------------
// setCurrentTapePool
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setCurrentTapePool(const std::string &currentTapePool) {
  m_currentTapePool = currentTapePool;
  m_currentTapePoolSet = true;
}

//------------------------------------------------------------------------------
// getCurrentTapePool
//------------------------------------------------------------------------------
std::string cta::dataStructures::DriveState::getCurrentTapePool() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_currentTapePool;
}

//------------------------------------------------------------------------------
// setCurrentVid
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setCurrentVid(const std::string &currentVid) {
  m_currentVid = currentVid;
  m_currentVidSet = true;
}

//------------------------------------------------------------------------------
// getCurrentVid
//------------------------------------------------------------------------------
std::string cta::dataStructures::DriveState::getCurrentVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_currentVid;
}

//------------------------------------------------------------------------------
// setFilesTransferedInSession
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setFilesTransferedInSession(const uint64_t filesTransferedInSession) {
  m_filesTransferedInSession = filesTransferedInSession;
  m_filesTransferedInSessionSet = true;
}

//------------------------------------------------------------------------------
// getFilesTransferedInSession
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::DriveState::getFilesTransferedInSession() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_filesTransferedInSession;
}

//------------------------------------------------------------------------------
// setLatestBandwidth
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setLatestBandwidth(const double &latestBandwidth) {
  m_latestBandwidth = latestBandwidth;
  m_latestBandwidthSet = true;
}

//------------------------------------------------------------------------------
// getLatestBandwidth
//------------------------------------------------------------------------------
double cta::dataStructures::DriveState::getLatestBandwidth() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_latestBandwidth;
}

//------------------------------------------------------------------------------
// setLogicalLibrary
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setLogicalLibrary(const std::string &logicalLibrary) {
  m_logicalLibrary = logicalLibrary;
  m_logicalLibrarySet = true;
}

//------------------------------------------------------------------------------
// getLogicalLibrary
//------------------------------------------------------------------------------
std::string cta::dataStructures::DriveState::getLogicalLibrary() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_logicalLibrary;
}

//------------------------------------------------------------------------------
// setMountType
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setMountType(const cta::dataStructures::MountType &mountType) {
  m_mountType = mountType;
  m_mountTypeSet = true;
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::dataStructures::MountType cta::dataStructures::DriveState::getMountType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_mountType;
}

//------------------------------------------------------------------------------
// setName
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setName(const std::string &name) {
  m_name = name;
  m_nameSet = true;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string cta::dataStructures::DriveState::getName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_name;
}

//------------------------------------------------------------------------------
// setSessionId
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setSessionId(const uint64_t sessionId) {
  m_sessionId = sessionId;
  m_sessionIdSet = true;
}

//------------------------------------------------------------------------------
// getSessionId
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::DriveState::getSessionId() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_sessionId;
}

//------------------------------------------------------------------------------
// setSessionStartTime
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setSessionStartTime(const time_t &sessionStartTime) {
  m_sessionStartTime = sessionStartTime;
  m_sessionStartTimeSet = true;
}

//------------------------------------------------------------------------------
// getSessionStartTime
//------------------------------------------------------------------------------
time_t cta::dataStructures::DriveState::getSessionStartTime() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_sessionStartTime;
}

//------------------------------------------------------------------------------
// setStatus
//------------------------------------------------------------------------------
void cta::dataStructures::DriveState::setStatus(const cta::dataStructures::DriveStatus &status) {
  m_status = status;
  m_statusSet = true;
}

//------------------------------------------------------------------------------
// getStatus
//------------------------------------------------------------------------------
cta::dataStructures::DriveStatus cta::dataStructures::DriveState::getStatus() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DriveState have been set!");
  }
  return m_status;
}
