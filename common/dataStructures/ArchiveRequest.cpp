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

#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveRequest::ArchiveRequest() {  
  m_checksumTypeSet = false;
  m_checksumValueSet = false;
  m_diskpoolNameSet = false;
  m_diskpoolThroughputSet = false;
  m_drDataSet = false;
  m_eosFileIDSet = false;
  m_fileSizeSet = false;
  m_requesterSet = false;
  m_srcURLSet = false;
  m_storageClassSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveRequest::~ArchiveRequest() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::ArchiveRequest::allFieldsSet() const {
  return m_checksumTypeSet
      && m_checksumValueSet
      && m_diskpoolNameSet
      && m_diskpoolThroughputSet
      && m_drDataSet
      && m_eosFileIDSet
      && m_fileSizeSet
      && m_requesterSet
      && m_srcURLSet
      && m_storageClassSet;
}

//------------------------------------------------------------------------------
// setChecksumType
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setChecksumType(const std::string &checksumType) {
  m_checksumType = checksumType;
  m_checksumTypeSet = true;
}

//------------------------------------------------------------------------------
// getChecksumType
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getChecksumType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_checksumType;
}

//------------------------------------------------------------------------------
// setChecksumValue
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setChecksumValue(const std::string &checksumValue) {
  m_checksumValue = checksumValue;
  m_checksumValueSet = true;
}

//------------------------------------------------------------------------------
// getChecksumValue
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getChecksumValue() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_checksumValue;
}

//------------------------------------------------------------------------------
// setDiskpoolName
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setDiskpoolName(const std::string &diskpoolName) {
  m_diskpoolName = diskpoolName;
  m_diskpoolNameSet = true;
}

//------------------------------------------------------------------------------
// getDiskpoolName
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getDiskpoolName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_diskpoolName;
}

//------------------------------------------------------------------------------
// setDiskpoolThroughput
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setDiskpoolThroughput(const std::string &diskpoolThroughput) {
  m_diskpoolThroughput = diskpoolThroughput;
  m_diskpoolThroughputSet = true;
}

//------------------------------------------------------------------------------
// getDiskpoolThroughput
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getDiskpoolThroughput() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_diskpoolThroughput;
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setDrData(const cta::dataStructures::DRData &drData) {
  m_drData = drData;
  m_drDataSet = true;
}

//------------------------------------------------------------------------------
// getDrData
//------------------------------------------------------------------------------
cta::dataStructures::DRData cta::dataStructures::ArchiveRequest::getDrData() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drData;
}

//------------------------------------------------------------------------------
// setEosFileID
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setEosFileID(const std::string &eosFileID) {
  m_eosFileID = eosFileID;
  m_eosFileIDSet = true;
}

//------------------------------------------------------------------------------
// getEosFileID
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getEosFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_eosFileID;
}

//------------------------------------------------------------------------------
// setFileSize
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setFileSize(const uint64_t fileSize) {
  m_fileSize = fileSize;
  m_fileSizeSet = true;
}

//------------------------------------------------------------------------------
// getFileSize
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::ArchiveRequest::getFileSize() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_fileSize;
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setRequester(const cta::dataStructures::Requester &requester) {
  m_requester = requester;
  m_requesterSet = true;
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::dataStructures::Requester cta::dataStructures::ArchiveRequest::getRequester() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_requester;
}

//------------------------------------------------------------------------------
// setSrcURL
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setSrcURL(const std::string &srcURL) {
  m_srcURL = srcURL;
  m_srcURLSet = true;
}

//------------------------------------------------------------------------------
// getSrcURL
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getSrcURL() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_srcURL;
}

//------------------------------------------------------------------------------
// setStorageClass
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveRequest::setStorageClass(const std::string &storageClass) {
  m_storageClass = storageClass;
  m_storageClassSet = true;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveRequest::getStorageClass() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_storageClass;
}
