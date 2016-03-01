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

#include "common/dataStructures/MountPolicy.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy::MountPolicy() {  
  m_maxDrivesSet = false;
  m_minBytesQueuedSet = false;
  m_minFilesQueuedSet = false;
  m_minRequestAgeSet = false;
  m_prioritySet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::MountPolicy::~MountPolicy() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::MountPolicy::allFieldsSet() const {
  return m_maxDrivesSet
      && m_minBytesQueuedSet
      && m_minFilesQueuedSet
      && m_minRequestAgeSet
      && m_prioritySet;
}

//------------------------------------------------------------------------------
// setMaxDrives
//------------------------------------------------------------------------------
void cta::common::dataStructures::MountPolicy::setMaxDrives(const uint64_t maxDrives) {
  m_maxDrives = maxDrives;
  m_maxDrivesSet = true;
}

//------------------------------------------------------------------------------
// getMaxDrives
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::MountPolicy::getMaxDrives() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the MountPolicy have been set!");
  }
  return m_maxDrives;
}

//------------------------------------------------------------------------------
// setMinBytesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::MountPolicy::setMinBytesQueued(const uint64_t minBytesQueued) {
  m_minBytesQueued = minBytesQueued;
  m_minBytesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getMinBytesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::MountPolicy::getMinBytesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the MountPolicy have been set!");
  }
  return m_minBytesQueued;
}

//------------------------------------------------------------------------------
// setMinFilesQueued
//------------------------------------------------------------------------------
void cta::common::dataStructures::MountPolicy::setMinFilesQueued(const uint64_t minFilesQueued) {
  m_minFilesQueued = minFilesQueued;
  m_minFilesQueuedSet = true;
}

//------------------------------------------------------------------------------
// getMinFilesQueued
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::MountPolicy::getMinFilesQueued() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the MountPolicy have been set!");
  }
  return m_minFilesQueued;
}

//------------------------------------------------------------------------------
// setMinRequestAge
//------------------------------------------------------------------------------
void cta::common::dataStructures::MountPolicy::setMinRequestAge(const uint64_t minRequestAge) {
  m_minRequestAge = minRequestAge;
  m_minRequestAgeSet = true;
}

//------------------------------------------------------------------------------
// getMinRequestAge
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::MountPolicy::getMinRequestAge() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the MountPolicy have been set!");
  }
  return m_minRequestAge;
}

//------------------------------------------------------------------------------
// setPriority
//------------------------------------------------------------------------------
void cta::common::dataStructures::MountPolicy::setPriority(const uint64_t priority) {
  m_priority = priority;
  m_prioritySet = true;
}

//------------------------------------------------------------------------------
// getPriority
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::MountPolicy::getPriority() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the MountPolicy have been set!");
  }
  return m_priority;
}
