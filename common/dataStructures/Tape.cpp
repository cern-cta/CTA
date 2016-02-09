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

#include "common/dataStructures/Tape.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::Tape::Tape() {  
  m_busySet = false;
  m_capacityInBytesSet = false;
  m_commentSet = false;
  m_creationLogSet = false;
  m_dataOnTapeInBytesSet = false;
  m_disabledSet = false;
  m_fullSet = false;
  m_lastFSeqSet = false;
  m_lastModificationLogSet = false;
  m_logicalLibraryNameSet = false;
  m_tapePoolNameSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::Tape::~Tape() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::Tape::allFieldsSet() const {
  return m_busySet
      && m_capacityInBytesSet
      && m_commentSet
      && m_creationLogSet
      && m_dataOnTapeInBytesSet
      && m_disabledSet
      && m_fullSet
      && m_lastFSeqSet
      && m_lastModificationLogSet
      && m_logicalLibraryNameSet
      && m_tapePoolNameSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setBusy
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setBusy(const bool &busy) {
  m_busy = busy;
  m_busySet = true;
}

//------------------------------------------------------------------------------
// getBusy
//------------------------------------------------------------------------------
bool cta::dataStructures::Tape::getBusy() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_busy;
}

//------------------------------------------------------------------------------
// setCapacityInBytes
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setCapacityInBytes(const uint64_t capacityInBytes) {
  m_capacityInBytes = capacityInBytes;
  m_capacityInBytesSet = true;
}

//------------------------------------------------------------------------------
// getCapacityInBytes
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::Tape::getCapacityInBytes() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_capacityInBytes;
}

//------------------------------------------------------------------------------
// setComment
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setComment(const std::string &comment) {
  m_comment = comment;
  m_commentSet = true;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
std::string cta::dataStructures::Tape::getComment() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_comment;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setCreationLog(const cta::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::Tape::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setDataOnTapeInBytes
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setDataOnTapeInBytes(const uint64_t dataOnTapeInBytes) {
  m_dataOnTapeInBytes = dataOnTapeInBytes;
  m_dataOnTapeInBytesSet = true;
}

//------------------------------------------------------------------------------
// getDataOnTapeInBytes
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::Tape::getDataOnTapeInBytes() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_dataOnTapeInBytes;
}

//------------------------------------------------------------------------------
// setDisabled
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setDisabled(const bool &disabled) {
  m_disabled = disabled;
  m_disabledSet = true;
}

//------------------------------------------------------------------------------
// getDisabled
//------------------------------------------------------------------------------
bool cta::dataStructures::Tape::getDisabled() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_disabled;
}

//------------------------------------------------------------------------------
// setFull
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setFull(const bool &full) {
  m_full = full;
  m_fullSet = true;
}

//------------------------------------------------------------------------------
// getFull
//------------------------------------------------------------------------------
bool cta::dataStructures::Tape::getFull() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_full;
}

//------------------------------------------------------------------------------
// setLastFSeq
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setLastFSeq(const uint64_t lastFSeq) {
  m_lastFSeq = lastFSeq;
  m_lastFSeqSet = true;
}

//------------------------------------------------------------------------------
// getLastFSeq
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::Tape::getLastFSeq() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_lastFSeq;
}

//------------------------------------------------------------------------------
// setLastModificationLog
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setLastModificationLog(const cta::dataStructures::EntryLog &lastModificationLog) {
  m_lastModificationLog = lastModificationLog;
  m_lastModificationLogSet = true;
}

//------------------------------------------------------------------------------
// getLastModificationLog
//------------------------------------------------------------------------------
cta::dataStructures::EntryLog cta::dataStructures::Tape::getLastModificationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_lastModificationLog;
}

//------------------------------------------------------------------------------
// setLogicalLibraryName
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setLogicalLibraryName(const std::string &logicalLibraryName) {
  m_logicalLibraryName = logicalLibraryName;
  m_logicalLibraryNameSet = true;
}

//------------------------------------------------------------------------------
// getLogicalLibraryName
//------------------------------------------------------------------------------
std::string cta::dataStructures::Tape::getLogicalLibraryName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_logicalLibraryName;
}

//------------------------------------------------------------------------------
// setTapePoolName
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setTapePoolName(const std::string &tapePoolName) {
  m_tapePoolName = tapePoolName;
  m_tapePoolNameSet = true;
}

//------------------------------------------------------------------------------
// getTapePoolName
//------------------------------------------------------------------------------
std::string cta::dataStructures::Tape::getTapePoolName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_tapePoolName;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::dataStructures::Tape::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::dataStructures::Tape::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Tape have been set!");
  }
  return m_vid;
}
