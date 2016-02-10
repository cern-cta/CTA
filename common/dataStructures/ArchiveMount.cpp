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
cta::common::dataStructures::ArchiveMount::ArchiveMount() {  
  m_lastFSeqSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::ArchiveMount::~ArchiveMount() throw() {
}

//------------------------------------------------------------------------------
// getNextJob
//------------------------------------------------------------------------------
std::unique_ptr<cta::common::dataStructures::ArchiveJob> cta::common::dataStructures::ArchiveMount::getNextJob() {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveMount have been set!");
  }
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
  return std::unique_ptr<cta::common::dataStructures::ArchiveJob>();
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::ArchiveMount::allFieldsSet() const {
  return m_lastFSeqSet;
}

//------------------------------------------------------------------------------
// setLastFSeq
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveMount::setLastFSeq(const uint64_t lastFSeq) {
  m_lastFSeq = lastFSeq;
  m_lastFSeqSet = true;
}

//------------------------------------------------------------------------------
// getLastFSeq
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::ArchiveMount::getLastFSeq() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveMount have been set!");
  }
  return m_lastFSeq;
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::common::dataStructures::ArchiveMount::complete() {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the ArchiveMount have been set!");
  }
  throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not implemented!");
}
