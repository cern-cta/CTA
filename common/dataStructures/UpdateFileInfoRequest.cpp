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

#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::UpdateFileInfoRequest::UpdateFileInfoRequest() {  
  m_archiveFileIDSet = false;
  m_drDataSet = false;
  m_requesterSet = false;
  m_storageClassSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::UpdateFileInfoRequest::~UpdateFileInfoRequest() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::UpdateFileInfoRequest::allFieldsSet() const {
  return m_archiveFileIDSet
      && m_drDataSet
      && m_requesterSet
      && m_storageClassSet;
}

//------------------------------------------------------------------------------
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::dataStructures::UpdateFileInfoRequest::setArchiveFileID(const std::string &archiveFileID) {
  m_archiveFileID = archiveFileID;
  m_archiveFileIDSet = true;
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
std::string cta::dataStructures::UpdateFileInfoRequest::getArchiveFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UpdateFileInfoRequest have been set!");
  }
  return m_archiveFileID;
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void cta::dataStructures::UpdateFileInfoRequest::setDrData(const cta::dataStructures::DRData &drData) {
  m_drData = drData;
  m_drDataSet = true;
}

//------------------------------------------------------------------------------
// getDrData
//------------------------------------------------------------------------------
cta::dataStructures::DRData cta::dataStructures::UpdateFileInfoRequest::getDrData() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UpdateFileInfoRequest have been set!");
  }
  return m_drData;
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void cta::dataStructures::UpdateFileInfoRequest::setRequester(const cta::dataStructures::Requester &requester) {
  m_requester = requester;
  m_requesterSet = true;
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::dataStructures::Requester cta::dataStructures::UpdateFileInfoRequest::getRequester() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UpdateFileInfoRequest have been set!");
  }
  return m_requester;
}

//------------------------------------------------------------------------------
// setStorageClass
//------------------------------------------------------------------------------
void cta::dataStructures::UpdateFileInfoRequest::setStorageClass(const std::string &storageClass) {
  m_storageClass = storageClass;
  m_storageClassSet = true;
}

//------------------------------------------------------------------------------
// getStorageClass
//------------------------------------------------------------------------------
std::string cta::dataStructures::UpdateFileInfoRequest::getStorageClass() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UpdateFileInfoRequest have been set!");
  }
  return m_storageClass;
}
