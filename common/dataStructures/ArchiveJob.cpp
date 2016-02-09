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

#include "common/dataStructures/ArchiveJob.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveJob::ArchiveJob() {  
  m_archiveFileIDSet = false;
  m_copyNumberSet = false;
  m_requestSet = false;
  m_tapePoolSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveJob::~ArchiveJob() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::ArchiveJob::allFieldsSet() const {
  return m_archiveFileIDSet
      && m_copyNumberSet
      && m_requestSet
      && m_tapePoolSet;
}

//------------------------------------------------------------------------------
// setArchiveFileID
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveJob::setArchiveFileID(const std::string &archiveFileID) {
  m_archiveFileID = archiveFileID;
  m_archiveFileIDSet = true;
}

//------------------------------------------------------------------------------
// getArchiveFileID
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveJob::getArchiveFileID() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveJob have been set!");
  }
  return m_archiveFileID;
}

//------------------------------------------------------------------------------
// setCopyNumber
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveJob::setCopyNumber(const std::string &copyNumber) {
  m_copyNumber = copyNumber;
  m_copyNumberSet = true;
}

//------------------------------------------------------------------------------
// getCopyNumber
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveJob::getCopyNumber() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveJob have been set!");
  }
  return m_copyNumber;
}

//------------------------------------------------------------------------------
// setRequest
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveJob::setRequest(const cta::dataStructures::ArchiveRequest &request) {
  m_request = request;
  m_requestSet = true;
}

//------------------------------------------------------------------------------
// getRequest
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveRequest cta::dataStructures::ArchiveJob::getRequest() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveJob have been set!");
  }
  return m_request;
}

//------------------------------------------------------------------------------
// setTapePool
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveJob::setTapePool(const std::string &tapePool) {
  m_tapePool = tapePool;
  m_tapePoolSet = true;
}

//------------------------------------------------------------------------------
// getTapePool
//------------------------------------------------------------------------------
std::string cta::dataStructures::ArchiveJob::getTapePool() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveJob have been set!");
  }
  return m_tapePool;
}
