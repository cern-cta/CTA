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

#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::RetrieveRequest::RetrieveRequest() {  
  m_archiveFileIDSet = false;
  m_diskpoolNameSet = false;
  m_diskpoolThroughputSet = false;
  m_drDataSet = false;
  m_dstURLSet = false;
  m_requesterSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::RetrieveRequest::~RetrieveRequest() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::RetrieveRequest::allFieldsSet() const {
  return m_archiveFileIDSet
      && m_diskpoolNameSet
      && m_diskpoolThroughputSet
      && m_drDataSet
      && m_dstURLSet
      && m_requesterSet;
}

//------------------------------------------------------------------------------
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setArchiveFileID(const std::string &archiveFileID) {
  m_archiveFileID = archiveFileID;
  m_archiveFileIDSet = true;
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
std::string cta::dataStructures::RetrieveRequest::getArchiveFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_archiveFileID;
}

//------------------------------------------------------------------------------
// setDiskpoolName
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setDiskpoolName(const std::string &diskpoolName) {
  m_diskpoolName = diskpoolName;
  m_diskpoolNameSet = true;
}

//------------------------------------------------------------------------------
// getDiskpoolName
//------------------------------------------------------------------------------
std::string cta::dataStructures::RetrieveRequest::getDiskpoolName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_diskpoolName;
}

//------------------------------------------------------------------------------
// setDiskpoolThroughput
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setDiskpoolThroughput(const std::string &diskpoolThroughput) {
  m_diskpoolThroughput = diskpoolThroughput;
  m_diskpoolThroughputSet = true;
}

//------------------------------------------------------------------------------
// getDiskpoolThroughput
//------------------------------------------------------------------------------
std::string cta::dataStructures::RetrieveRequest::getDiskpoolThroughput() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_diskpoolThroughput;
}

//------------------------------------------------------------------------------
// setDrData
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setDrData(const cta::dataStructures::DRData &drData) {
  m_drData = drData;
  m_drDataSet = true;
}

//------------------------------------------------------------------------------
// getDrData
//------------------------------------------------------------------------------
cta::dataStructures::DRData cta::dataStructures::RetrieveRequest::getDrData() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_drData;
}

//------------------------------------------------------------------------------
// setDstURL
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setDstURL(const std::string &dstURL) {
  m_dstURL = dstURL;
  m_dstURLSet = true;
}

//------------------------------------------------------------------------------
// getDstURL
//------------------------------------------------------------------------------
std::string cta::dataStructures::RetrieveRequest::getDstURL() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_dstURL;
}

//------------------------------------------------------------------------------
// setRequester
//------------------------------------------------------------------------------
void cta::dataStructures::RetrieveRequest::setRequester(const cta::dataStructures::Requester &requester) {
  m_requester = requester;
  m_requesterSet = true;
}

//------------------------------------------------------------------------------
// getRequester
//------------------------------------------------------------------------------
cta::dataStructures::Requester cta::dataStructures::RetrieveRequest::getRequester() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the RetrieveRequest have been set!");
  }
  return m_requester;
}
