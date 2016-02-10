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

#include "common/dataStructures/TapeFileLocation.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeFileLocation::TapeFileLocation() {  
  m_blockIdSet = false;
  m_fSeqSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::dataStructures::TapeFileLocation::~TapeFileLocation() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::common::dataStructures::TapeFileLocation::allFieldsSet() const {
  return m_blockIdSet
      && m_fSeqSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setBlockId
//------------------------------------------------------------------------------
void cta::common::dataStructures::TapeFileLocation::setBlockId(const uint64_t blockId) {
  m_blockId = blockId;
  m_blockIdSet = true;
}

//------------------------------------------------------------------------------
// getBlockId
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::TapeFileLocation::getBlockId() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeFileLocation have been set!");
  }
  return m_blockId;
}

//------------------------------------------------------------------------------
// setFSeq
//------------------------------------------------------------------------------
void cta::common::dataStructures::TapeFileLocation::setFSeq(const uint64_t fSeq) {
  m_fSeq = fSeq;
  m_fSeqSet = true;
}

//------------------------------------------------------------------------------
// getFSeq
//------------------------------------------------------------------------------
uint64_t cta::common::dataStructures::TapeFileLocation::getFSeq() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeFileLocation have been set!");
  }
  return m_fSeq;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::common::dataStructures::TapeFileLocation::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::common::dataStructures::TapeFileLocation::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeFileLocation have been set!");
  }
  return m_vid;
}
