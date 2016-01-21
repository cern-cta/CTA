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

#include "common/ArchiveRequest.hpp"
#include "exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveRequest::ArchiveRequest() {  
  m_srcURLSet = false;
  m_fileSizeSet = false;
  m_checksumTypeSet = false;
  m_checksumValueSet = false;
  m_storageClassSet = false;
  m_drInstanceSet = false;
  m_drPathSet = false;
  m_drOwnerSet = false;
  m_drGroupSet = false;
  m_drBlobSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveRequest::~ArchiveRequest() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::ArchiveRequest::allFieldsSet() const {
  if(m_srcURLSet==true 
          && m_fileSizeSet==true 
          && m_checksumTypeSet==true 
          && m_checksumValueSet==true 
          && m_storageClassSet==true 
          && m_drInstanceSet==true 
          && m_drPathSet==true 
          && m_drOwnerSet==true 
          && m_drGroupSet==true 
          && m_drBlobSet==true) {
    return true;
  }
  return false;
}

//------------------------------------------------------------------------------
// setDrBlob
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setDrBlob(std::string drBlob) {
  m_drBlob = drBlob;
  m_drBlobSet = true;
}

//------------------------------------------------------------------------------
// getDrBlob
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getDrBlob() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drBlob;
}

//------------------------------------------------------------------------------
// setDrGroup
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setDrGroup(std::string drGroup) {
  m_drGroup = drGroup;
  m_drGroupSet = true;
}

//------------------------------------------------------------------------------
// getDrGroup
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getDrGroup() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drGroup;
}

//------------------------------------------------------------------------------
// setDrOwner
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setDrOwner(std::string drOwner) {
  m_drOwner = drOwner;
  m_drOwnerSet = true;
}

//------------------------------------------------------------------------------
// getDrOwner
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getDrOwner() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drOwner;
}

//------------------------------------------------------------------------------
// setDrPath
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setDrPath(std::string drPath) {
  m_drPath = drPath;
  m_drPathSet = true;
}

//------------------------------------------------------------------------------
// getDrPath
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getDrPath() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drPath;
}

//------------------------------------------------------------------------------
// setDrInstance
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setDrInstance(std::string drInstance) {
  m_drInstance = drInstance;
  m_drInstanceSet = true;
}

//------------------------------------------------------------------------------
// getDrInstance
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getDrInstance() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_drInstance;
}

//------------------------------------------------------------------------------
// setStorageClass
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setStorageClass(std::string storageClass) {
  m_storageClass = storageClass;
  m_storageClassSet = true;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getStorageClass() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_storageClass;
}

//------------------------------------------------------------------------------
// setChecksumValue
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setChecksumValue(std::string checksumValue) {
  m_checksumValue = checksumValue;
  m_checksumValueSet = true;
}

//------------------------------------------------------------------------------
// getChecksumValue
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getChecksumValue() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_checksumValue;
}

//------------------------------------------------------------------------------
// setChecksumType
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setChecksumType(std::string checksumType) {
  m_checksumType = checksumType;
  m_checksumTypeSet = true;
}

//------------------------------------------------------------------------------
// getChecksumType
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getChecksumType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_checksumType;
}

//------------------------------------------------------------------------------
// setFileSize
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setFileSize(uint64_t fileSize) {
  m_fileSize = fileSize;
  m_fileSizeSet = true;
}

//------------------------------------------------------------------------------
// getFileSize
//------------------------------------------------------------------------------
uint64_t cta::ArchiveRequest::getFileSize() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_fileSize;
}

//------------------------------------------------------------------------------
// setSrcURL
//------------------------------------------------------------------------------
void cta::ArchiveRequest::setSrcURL(std::string srcURL) {
  m_srcURL = srcURL; 
  m_srcURLSet = true;
}

//------------------------------------------------------------------------------
// getSrcURL
//------------------------------------------------------------------------------
std::string cta::ArchiveRequest::getSrcURL() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveRequest have been set!");
  }
  return m_srcURL;
}
