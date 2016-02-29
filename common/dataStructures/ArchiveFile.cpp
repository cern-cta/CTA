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

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile::ArchiveFile() {  
  m_archiveFileIDSet = false;
  m_checksumTypeSet = false;
  m_checksumValueSet = false;
  m_drDataSet = false;
  m_eosFileIDSet = false;
  m_fileSizeSet = false;
  m_storageClassSet = false;
  m_tapeCopiesSet = false;
  m_creationLogSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveFile::~ArchiveFile() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ArchiveFile::allFieldsSet() const {
  return m_archiveFileIDSet
      && m_checksumTypeSet
      && m_checksumValueSet
      && m_creationLogSet
      && m_drDataSet
      && m_eosFileIDSet
      && m_fileSizeSet
      && m_storageClassSet
      && m_tapeCopiesSet;
}

//------------------------------------------------------------------------------
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setArchiveFileID(const std::string &archiveFileID) {
  m_archiveFileID = archiveFileID;
  m_archiveFileIDSet = true;
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ArchiveFile::getArchiveFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_archiveFileID;
}

//------------------------------------------------------------------------------
// setChecksumType
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setChecksumType(const std::string &checksumType) {
  m_checksumType = checksumType;
  m_checksumTypeSet = true;
}

//------------------------------------------------------------------------------
// getChecksumType
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ArchiveFile::getChecksumType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_checksumType;
}

//------------------------------------------------------------------------------
// setChecksumValue
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setChecksumValue(const std::string &checksumValue) {
  m_checksumValue = checksumValue;
  m_checksumValueSet = true;
}

//------------------------------------------------------------------------------
// getChecksumValue
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ArchiveFile::getChecksumValue() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_checksumValue;
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setDrData(const cta::common::dataStructures::DRData &drData) {
  m_drData = drData;
  m_drDataSet = true;
}

//------------------------------------------------------------------------------
// getDrData
//------------------------------------------------------------------------------
cta::common::dataStructures::DRData cta::common::dataStructures::ArchiveFile::getDrData() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_drData;
}

//------------------------------------------------------------------------------
// setEosFileID
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setEosFileID(const std::string &eosFileID) {
  m_eosFileID = eosFileID;
  m_eosFileIDSet = true;
}

//------------------------------------------------------------------------------
// getEosFileID
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ArchiveFile::getEosFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_eosFileID;
}

//------------------------------------------------------------------------------
// setFileSize
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setFileSize(const uint64_t fileSize) {
  m_fileSize = fileSize;
  m_fileSizeSet = true;
}

//------------------------------------------------------------------------------
// getFileSize
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ArchiveFile::getFileSize() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_fileSize;
}

//------------------------------------------------------------------------------
// setStorageClass
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setStorageClass(const std::string &storageClass) {
  m_storageClass = storageClass;
  m_storageClassSet = true;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ArchiveFile::getStorageClass() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_storageClass;
}

//------------------------------------------------------------------------------
// setTapeCopies
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setTapeCopies(const std::map<int,cta::common::dataStructures::TapeFileLocation> &tapeCopies) {
  m_tapeCopies = tapeCopies;
  m_tapeCopiesSet = true;
}

//------------------------------------------------------------------------------
// getTapeCopies
//------------------------------------------------------------------------------
std::map<int,cta::common::dataStructures::TapeFileLocation> cta::common::dataStructures::ArchiveFile::getTapeCopies() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveFile have been set!");
  }
  return m_tapeCopies;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveFile::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::ArchiveFile::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the AdminHost have been set!");
  }
  return m_creationLog;
}
