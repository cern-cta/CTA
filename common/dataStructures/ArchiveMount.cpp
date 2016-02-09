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

#include "common/dataStructures/ArchiveMount.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveMount::ArchiveMount() {  
  m_jobsSet = false;
  m_lastFSeqSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::ArchiveMount::~ArchiveMount() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::ArchiveMount::allFieldsSet() const {
  return m_jobsSet
      && m_lastFSeqSet;
}

//------------------------------------------------------------------------------
// setJobs
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveMount::setJobs(const std::list<cta::dataStructures::ArchiveJob> &jobs) {
  m_jobs = jobs;
  m_jobsSet = true;
}

//------------------------------------------------------------------------------
// getJobs
//------------------------------------------------------------------------------
std::list<cta::dataStructures::ArchiveJob> cta::dataStructures::ArchiveMount::getJobs() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveMount have been set!");
  }
  return m_jobs;
}

//------------------------------------------------------------------------------
// setLastFSeq
//------------------------------------------------------------------------------
void cta::dataStructures::ArchiveMount::setLastFSeq(const uint64_t lastFSeq) {
  m_lastFSeq = lastFSeq;
  m_lastFSeqSet = true;
}

//------------------------------------------------------------------------------
// getLastFSeq
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::ArchiveMount::getLastFSeq() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveMount have been set!");
  }
  return m_lastFSeq;
}
