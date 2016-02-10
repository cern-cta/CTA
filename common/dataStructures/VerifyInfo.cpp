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

#include "common/dataStructures/VerifyInfo.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyInfo::VerifyInfo() {  
  m_creationLogSet = false;
  m_filesFailedSet = false;
  m_filesToVerifySet = false;
  m_filesVerifiedSet = false;
  m_tagSet = false;
  m_totalFilesSet = false;
  m_totalSizeSet = false;
  m_verifyStatusSet = false;
  m_verifyTypeSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyInfo::~VerifyInfo() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::VerifyInfo::allFieldsSet() const {
  return m_creationLogSet
      && m_filesFailedSet
      && m_filesToVerifySet
      && m_filesVerifiedSet
      && m_tagSet
      && m_totalFilesSet
      && m_totalSizeSet
      && m_verifyStatusSet
      && m_verifyTypeSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::VerifyInfo::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setFilesFailed
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setFilesFailed(const uint64_t filesFailed) {
  m_filesFailed = filesFailed;
  m_filesFailedSet = true;
}

//------------------------------------------------------------------------------
// getFilesFailed
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::VerifyInfo::getFilesFailed() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_filesFailed;
}

//------------------------------------------------------------------------------
// setFilesToVerify
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setFilesToVerify(const uint64_t filesToVerify) {
  m_filesToVerify = filesToVerify;
  m_filesToVerifySet = true;
}

//------------------------------------------------------------------------------
// getFilesToVerify
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::VerifyInfo::getFilesToVerify() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_filesToVerify;
}

//------------------------------------------------------------------------------
// setFilesVerified
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setFilesVerified(const uint64_t filesVerified) {
  m_filesVerified = filesVerified;
  m_filesVerifiedSet = true;
}

//------------------------------------------------------------------------------
// getFilesVerified
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::VerifyInfo::getFilesVerified() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_filesVerified;
}

//------------------------------------------------------------------------------
// setTag
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setTag(const std::string &tag) {
  m_tag = tag;
  m_tagSet = true;
}

//------------------------------------------------------------------------------
// getTag
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::VerifyInfo::getTag() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_tag;
}

//------------------------------------------------------------------------------
// setTotalFiles
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setTotalFiles(const uint64_t totalFiles) {
  m_totalFiles = totalFiles;
  m_totalFilesSet = true;
}

//------------------------------------------------------------------------------
// getTotalFiles
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::VerifyInfo::getTotalFiles() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_totalFiles;
}

//------------------------------------------------------------------------------
// setTotalSize
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setTotalSize(const uint64_t totalSize) {
  m_totalSize = totalSize;
  m_totalSizeSet = true;
}

//------------------------------------------------------------------------------
// getTotalSize
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::VerifyInfo::getTotalSize() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_totalSize;
}

//------------------------------------------------------------------------------
// setVerifyStatus
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setVerifyStatus(const std::string &verifyStatus) {
  m_verifyStatus = verifyStatus;
  m_verifyStatusSet = true;
}

//------------------------------------------------------------------------------
// getVerifyStatus
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::VerifyInfo::getVerifyStatus() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_verifyStatus;
}

//------------------------------------------------------------------------------
// setVerifyType
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setVerifyType(const cta::common::dataStructures::VerifyType &verifyType) {
  m_verifyType = verifyType;
  m_verifyTypeSet = true;
}

//------------------------------------------------------------------------------
// getVerifyType
//------------------------------------------------------------------------------
cta::common::dataStructures::VerifyType cta::common::dataStructures::VerifyInfo::getVerifyType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_verifyType;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::VerifyInfo::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::VerifyInfo::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the VerifyInfo have been set!");
  }
  return m_vid;
}
