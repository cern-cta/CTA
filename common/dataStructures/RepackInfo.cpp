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

#include "common/dataStructures/RepackInfo.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo::RepackInfo() {  
  m_creationLogSet = false;
  m_errorsSet = false;
  m_filesFailedSet = false;
  m_filesArchivedSet = false;
  m_filesToArchiveSet = false;
  m_filesToRetrieveSet = false;
  m_repackStatusSet = false;
  m_repackTypeSet = false;
  m_tagSet = false;
  m_totalFilesSet = false;
  m_totalSizeSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackInfo::~RepackInfo() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::RepackInfo::allFieldsSet() const {
  return m_creationLogSet
      && m_errorsSet
      && m_filesFailedSet
      && m_filesArchivedSet
      && m_filesToArchiveSet
      && m_filesToRetrieveSet
      && m_repackStatusSet
      && m_repackTypeSet
      && m_tagSet
      && m_totalFilesSet
      && m_totalSizeSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::RepackInfo::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setErrors
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setErrors(const std::map<uint64_t,std::string> &errors) {
  m_errors = errors;
  m_errorsSet = true;
}

//------------------------------------------------------------------------------
// getErrors
//------------------------------------------------------------------------------
std::map<uint64_t,std::string> cta::common::dataStructures::RepackInfo::getErrors() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_errors;
}

//------------------------------------------------------------------------------
// setFilesFailed
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setFilesFailed(const uint64_t filesFailed) {
  m_filesFailed = filesFailed;
  m_filesFailedSet = true;
}

//------------------------------------------------------------------------------
// getFilesFailed
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getFilesFailed() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_filesFailed;
}

//------------------------------------------------------------------------------
// setFilesMigrated
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setFilesArchived(const uint64_t filesArchived) {
  m_filesArchived = filesArchived;
  m_filesArchivedSet = true;
}

//------------------------------------------------------------------------------
// getFilesMigrated
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getFilesArchived() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_filesArchived;
}

//------------------------------------------------------------------------------
// setFilesToMigr
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setFilesToArchive(const uint64_t filesToArchive) {
  m_filesToArchive = filesToArchive;
  m_filesToArchiveSet = true;
}

//------------------------------------------------------------------------------
// getFilesToMigr
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getFilesToArchive() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_filesToArchive;
}

//------------------------------------------------------------------------------
// setFilesToRecall
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setFilesToRetrieve(const uint64_t filesToRetrieve) {
  m_filesToRetrieve = filesToRetrieve;
  m_filesToRetrieveSet = true;
}

//------------------------------------------------------------------------------
// getFilesToRecall
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getFilesToRetrieve() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_filesToRetrieve;
}

//------------------------------------------------------------------------------
// setRepackStatus
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setRepackStatus(const std::string &repackStatus) {
  m_repackStatus = repackStatus;
  m_repackStatusSet = true;
}

//------------------------------------------------------------------------------
// getRepackStatus
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::RepackInfo::getRepackStatus() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_repackStatus;
}

//------------------------------------------------------------------------------
// setRepackType
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setRepackType(const cta::common::dataStructures::RepackType &repackType) {
  m_repackType = repackType;
  m_repackTypeSet = true;
}

//------------------------------------------------------------------------------
// getRepackType
//------------------------------------------------------------------------------
cta::common::dataStructures::RepackType cta::common::dataStructures::RepackInfo::getRepackType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_repackType;
}

//------------------------------------------------------------------------------
// setTag
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setTag(const std::string &tag) {
  m_tag = tag;
  m_tagSet = true;
}

//------------------------------------------------------------------------------
// getTag
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::RepackInfo::getTag() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_tag;
}

//------------------------------------------------------------------------------
// setTotalFiles
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setTotalFiles(const uint64_t totalFiles) {
  m_totalFiles = totalFiles;
  m_totalFilesSet = true;
}

//------------------------------------------------------------------------------
// getTotalFiles
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getTotalFiles() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_totalFiles;
}

//------------------------------------------------------------------------------
// setTotalSize
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setTotalSize(const uint64_t totalSize) {
  m_totalSize = totalSize;
  m_totalSizeSet = true;
}

//------------------------------------------------------------------------------
// getTotalSize
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::RepackInfo::getTotalSize() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_totalSize;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::RepackInfo::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::RepackInfo::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RepackInfo have been set!");
  }
  return m_vid;
}
