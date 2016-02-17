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

#include "common/dataStructures/WriteTestResult.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult::WriteTestResult() {  
  m_checksumsSet = false;
  m_driveNameSet = false;
  m_errorsSet = false;
  m_totalBytesWrittenSet = false;
  m_totalFilesWrittenSet = false;
  m_totalTimeInSecondsSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::WriteTestResult::~WriteTestResult() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::WriteTestResult::allFieldsSet() const {
  return m_checksumsSet
      && m_driveNameSet
      && m_errorsSet
      && m_totalBytesWrittenSet
      && m_totalFilesWrittenSet
      && m_totalTimeInSecondsSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setChecksums
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setChecksums(const std::map<int,std::pair<std::string,std::string>> &checksums) {
  m_checksums = checksums;
  m_checksumsSet = true;
}

//------------------------------------------------------------------------------
// getChecksums
//------------------------------------------------------------------------------
std::map<int,std::pair<std::string,std::string>> cta::common::dataStructures::WriteTestResult::getChecksums() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_checksums;
}

//------------------------------------------------------------------------------
// setDriveName
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setDriveName(const std::string &driveName) {
  m_driveName = driveName;
  m_driveNameSet = true;
}

//------------------------------------------------------------------------------
// getDriveName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::WriteTestResult::getDriveName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_driveName;
}

//------------------------------------------------------------------------------
// setErrors
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setErrors(const std::map<int,std::string> &errors) {
  m_errors = errors;
  m_errorsSet = true;
}

//------------------------------------------------------------------------------
// getErrors
//------------------------------------------------------------------------------
std::map<int,std::string> cta::common::dataStructures::WriteTestResult::getErrors() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_errors;
}

//------------------------------------------------------------------------------
// setTotalBytesWritten
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setTotalBytesWritten(const uint64_t totalBytesWritten) {
  m_totalBytesWritten = totalBytesWritten;
  m_totalBytesWrittenSet = true;
}

//------------------------------------------------------------------------------
// getTotalBytesWritten
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::WriteTestResult::getTotalBytesWritten() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_totalBytesWritten;
}

//------------------------------------------------------------------------------
// setTotalFilesWritten
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setTotalFilesWritten(const uint64_t totalFilesWritten) {
  m_totalFilesWritten = totalFilesWritten;
  m_totalFilesWrittenSet = true;
}

//------------------------------------------------------------------------------
// getTotalFilesWritten
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::WriteTestResult::getTotalFilesWritten() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_totalFilesWritten;
}

//------------------------------------------------------------------------------
// setTotalTimeInSeconds
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setTotalTimeInSeconds(const uint64_t totalTimeInSeconds) {
  m_totalTimeInSeconds = totalTimeInSeconds;
  m_totalTimeInSecondsSet = true;
}

//------------------------------------------------------------------------------
// getTotalTimeInSeconds
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::WriteTestResult::getTotalTimeInSeconds() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_totalTimeInSeconds;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::WriteTestResult::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::WriteTestResult::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the WriteTestResult have been set!");
  }
  return m_vid;
}
