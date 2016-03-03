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

#include "common/dataStructures/Dedication.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::Dedication::Dedication() {  
  m_commentSet = false;
  m_creationLogSet = false;
  m_dedicationTypeSet = false;
  m_driveNameSet = false;
  m_fromTimestampSet = false;
  m_lastModificationLogSet = false;
  m_tagSet = false;
  m_untilTimestampSet = false;
  m_mountGroupSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::Dedication::~Dedication() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::Dedication::allFieldsSet() const {
  return m_commentSet
      && m_creationLogSet
      && m_dedicationTypeSet
      && m_driveNameSet
      && m_fromTimestampSet
      && m_lastModificationLogSet
      && m_tagSet
      && m_untilTimestampSet
      && m_mountGroupSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setComment
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setComment(const std::string &comment) {
  m_comment = comment;
  m_commentSet = true;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Dedication::getComment() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_comment;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::Dedication::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setDedicationType
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setDedicationType(const cta::common::dataStructures::DedicationType &dedicationType) {
  m_dedicationType = dedicationType;
  m_dedicationTypeSet = true;
}

//------------------------------------------------------------------------------
// getDedicationType
//------------------------------------------------------------------------------
cta::common::dataStructures::DedicationType cta::common::dataStructures::Dedication::getDedicationType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_dedicationType;
}

//------------------------------------------------------------------------------
// setDriveName
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setDriveName(const std::string &driveName) {
  m_driveName = driveName;
  m_driveNameSet = true;
}

//------------------------------------------------------------------------------
// getDriveName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Dedication::getDriveName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_driveName;
}

//------------------------------------------------------------------------------
// setFromTimestamp
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setFromTimestamp(const uint64_t fromTimestamp) {
  m_fromTimestamp = fromTimestamp;
  m_fromTimestampSet = true;
}

//------------------------------------------------------------------------------
// getFromTimestamp
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::Dedication::getFromTimestamp() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_fromTimestamp;
}

//------------------------------------------------------------------------------
// setLastModificationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setLastModificationLog(const cta::common::dataStructures::EntryLog &lastModificationLog) {
  m_lastModificationLog = lastModificationLog;
  m_lastModificationLogSet = true;
}

//------------------------------------------------------------------------------
// getLastModificationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::Dedication::getLastModificationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_lastModificationLog;
}

//------------------------------------------------------------------------------
// setTag
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setTag(const std::string &tag) {
  m_tag = tag;
  m_tagSet = true;
}

//------------------------------------------------------------------------------
// getTag
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Dedication::getTag() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_tag;
}

//------------------------------------------------------------------------------
// setUntilTimestamp
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setUntilTimestamp(const uint64_t untilTimestamp) {
  m_untilTimestamp = untilTimestamp;
  m_untilTimestampSet = true;
}

//------------------------------------------------------------------------------
// getUntilTimestamp
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::Dedication::getUntilTimestamp() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_untilTimestamp;
}

//------------------------------------------------------------------------------
// setMountGroup
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setMountGroup(const std::string &mountGroup) {
  m_mountGroup = mountGroup;
  m_mountGroupSet = true;
}

//------------------------------------------------------------------------------
// getMountGroup
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Dedication::getMountGroup() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_mountGroup;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::Dedication::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::Dedication::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the Dedication have been set!");
  }
  return m_vid;
}
