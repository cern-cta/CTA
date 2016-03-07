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

#include "common/dataStructures/ReadTestResult.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ReadTestResult::ReadTestResult() {  
  m_checksumsSet = false;
  m_driveNameSet = false;
  m_errorsSet = false;
  m_noOfFilesReadSet = false;
  m_totalBytesReadSet = false;
  m_totalFilesReadSet = false;
  m_totalTimeInSecondsSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ReadTestResult::~ReadTestResult() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ReadTestResult::allFieldsSet() const {
  return m_checksumsSet
      && m_driveNameSet
      && m_errorsSet
      && m_noOfFilesReadSet
      && m_totalBytesReadSet
      && m_totalFilesReadSet
      && m_totalTimeInSecondsSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setChecksums
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setChecksums(const std::map<uint64_t,std::pair<std::string,std::string>> &checksums) {
  m_checksums = checksums;
  m_checksumsSet = true;
}

//------------------------------------------------------------------------------
// getChecksums
//------------------------------------------------------------------------------
std::map<uint64_t,std::pair<std::string,std::string>> cta::common::dataStructures::ReadTestResult::getChecksums() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_checksums;
}

//------------------------------------------------------------------------------
// setDriveName
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setDriveName(const std::string &driveName) {
  m_driveName = driveName;
  m_driveNameSet = true;
}

//------------------------------------------------------------------------------
// getDriveName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ReadTestResult::getDriveName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_driveName;
}

//------------------------------------------------------------------------------
// setErrors
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setErrors(const std::map<uint64_t,std::string> &errors) {
  m_errors = errors;
  m_errorsSet = true;
}

//------------------------------------------------------------------------------
// getErrors
//------------------------------------------------------------------------------
std::map<uint64_t,std::string> cta::common::dataStructures::ReadTestResult::getErrors() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_errors;
}

//------------------------------------------------------------------------------
// setNoOfFilesRead
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setNoOfFilesRead(const uint64_t noOfFilesRead) {
  m_noOfFilesRead = noOfFilesRead;
  m_noOfFilesReadSet = true;
}

//------------------------------------------------------------------------------
// getNoOfFilesRead
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ReadTestResult::getNoOfFilesRead() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_noOfFilesRead;
}

//------------------------------------------------------------------------------
// setTotalBytesRead
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setTotalBytesRead(const uint64_t totalBytesRead) {
  m_totalBytesRead = totalBytesRead;
  m_totalBytesReadSet = true;
}

//------------------------------------------------------------------------------
// getTotalBytesRead
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ReadTestResult::getTotalBytesRead() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_totalBytesRead;
}

//------------------------------------------------------------------------------
// setTotalFilesRead
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setTotalFilesRead(const uint64_t totalFilesRead) {
  m_totalFilesRead = totalFilesRead;
  m_totalFilesReadSet = true;
}

//------------------------------------------------------------------------------
// getTotalFilesRead
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ReadTestResult::getTotalFilesRead() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_totalFilesRead;
}

//------------------------------------------------------------------------------
// setTotalTimeInSeconds
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setTotalTimeInSeconds(const uint64_t totalTimeInSeconds) {
  m_totalTimeInSeconds = totalTimeInSeconds;
  m_totalTimeInSecondsSet = true;
}

//------------------------------------------------------------------------------
// getTotalTimeInSeconds
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ReadTestResult::getTotalTimeInSeconds() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_totalTimeInSeconds;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::ReadTestResult::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::ReadTestResult::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ReadTestResult have been set!");
  }
  return m_vid;
}
