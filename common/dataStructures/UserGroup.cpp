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

#include "common/dataStructures/UserGroup.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::UserGroup::UserGroup() {  
  m_archive_minBytesQueuedSet = false;
  m_archive_minFilesQueuedSet = false;
  m_archive_minRequestAgeSet = false;
  m_archive_prioritySet = false;
  m_commentSet = false;
  m_creationLogSet = false;
  m_lastModificationLogSet = false;
  m_maxDrivesAllowedSet = false;
  m_nameSet = false;
  m_retrieve_minBytesQueuedSet = false;
  m_retrieve_minFilesQueuedSet = false;
  m_retrieve_minRequestAgeSet = false;
  m_retrieve_prioritySet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::UserGroup::~UserGroup() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::UserGroup::allFieldsSet() const {
  return m_archive_minBytesQueuedSet
      && m_archive_minFilesQueuedSet
      && m_archive_minRequestAgeSet
      && m_archive_prioritySet
      && m_commentSet
      && m_creationLogSet
      && m_lastModificationLogSet
      && m_maxDrivesAllowedSet
      && m_nameSet
      && m_retrieve_minBytesQueuedSet
      && m_retrieve_minFilesQueuedSet
      && m_retrieve_minRequestAgeSet
      && m_retrieve_prioritySet;
}

//------------------------------------------------------------------------------
// setArchive_minBytesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setArchive_minBytesQueued(const uint64_t archive_minBytesQueued) {
  m_archive_minBytesQueued = archive_minBytesQueued;
  m_archive_minBytesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getArchive_minBytesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getArchive_minBytesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_archive_minBytesQueued;
}

//------------------------------------------------------------------------------
// setArchive_minFilesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setArchive_minFilesQueued(const uint64_t archive_minFilesQueued) {
  m_archive_minFilesQueued = archive_minFilesQueued;
  m_archive_minFilesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getArchive_minFilesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getArchive_minFilesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_archive_minFilesQueued;
}

//------------------------------------------------------------------------------
// setArchive_minRequestAge
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setArchive_minRequestAge(const uint64_t archive_minRequestAge) {
  m_archive_minRequestAge = archive_minRequestAge;
  m_archive_minRequestAgeSet = true;
}

//------------------------------------------------------------------------------
// getArchive_minRequestAge
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getArchive_minRequestAge() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_archive_minRequestAge;
}

//------------------------------------------------------------------------------
// setArchive_priority
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setArchive_priority(const uint64_t archive_priority) {
  m_archive_priority = archive_priority;
  m_archive_prioritySet = true;
}

//------------------------------------------------------------------------------
// getArchive_priority
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getArchive_priority() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_archive_priority;
}

//------------------------------------------------------------------------------
// setComment
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setComment(const std::string &comment) {
  m_comment = comment;
  m_commentSet = true;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::UserGroup::getComment() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_comment;
}

//------------------------------------------------------------------------------
// setCreationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setCreationLog(const cta::common::dataStructures::EntryLog &creationLog) {
  m_creationLog = creationLog;
  m_creationLogSet = true;
}

//------------------------------------------------------------------------------
// getCreationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::UserGroup::getCreationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_creationLog;
}

//------------------------------------------------------------------------------
// setLastModificationLog
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setLastModificationLog(const cta::common::dataStructures::EntryLog &lastModificationLog) {
  m_lastModificationLog = lastModificationLog;
  m_lastModificationLogSet = true;
}

//------------------------------------------------------------------------------
// getLastModificationLog
//------------------------------------------------------------------------------
cta::common::dataStructures::EntryLog cta::common::dataStructures::UserGroup::getLastModificationLog() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_lastModificationLog;
}

//------------------------------------------------------------------------------
// setMaxDrivesAllowed
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setMaxDrivesAllowed(const uint64_t maxDrivesAllowed) {
  m_maxDrivesAllowed = maxDrivesAllowed;
  m_maxDrivesAllowedSet = true;
}

//------------------------------------------------------------------------------
// getMaxDrivesAllowed
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getMaxDrivesAllowed() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_maxDrivesAllowed;
}

//------------------------------------------------------------------------------
// setName
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setName(const std::string &name) {
  m_name = name;
  m_nameSet = true;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::UserGroup::getName() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_name;
}

//------------------------------------------------------------------------------
// setRetrieve_minBytesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setRetrieve_minBytesQueued(const uint64_t retrieve_minBytesQueued) {
  m_retrieve_minBytesQueued = retrieve_minBytesQueued;
  m_retrieve_minBytesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getRetrieve_minBytesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getRetrieve_minBytesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_retrieve_minBytesQueued;
}

//------------------------------------------------------------------------------
// setRetrieve_minFilesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setRetrieve_minFilesQueued(const uint64_t retrieve_minFilesQueued) {
  m_retrieve_minFilesQueued = retrieve_minFilesQueued;
  m_retrieve_minFilesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getRetrieve_minFilesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getRetrieve_minFilesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_retrieve_minFilesQueued;
}

//------------------------------------------------------------------------------
// setRetrieve_minRequestAge
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setRetrieve_minRequestAge(const uint64_t retrieve_minRequestAge) {
  m_retrieve_minRequestAge = retrieve_minRequestAge;
  m_retrieve_minRequestAgeSet = true;
}

//------------------------------------------------------------------------------
// getRetrieve_minRequestAge
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getRetrieve_minRequestAge() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_retrieve_minRequestAge;
}

//------------------------------------------------------------------------------
// setRetrieve_priority
//------------------------------------------------------------------------------
void cta::common::dataStructures::UserGroup::setRetrieve_priority(const uint64_t retrieve_priority) {
  m_retrieve_priority = retrieve_priority;
  m_retrieve_prioritySet = true;
}

//------------------------------------------------------------------------------
// getRetrieve_priority
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::UserGroup::getRetrieve_priority() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the UserGroup have been set!");
  }
  return m_retrieve_priority;
}
