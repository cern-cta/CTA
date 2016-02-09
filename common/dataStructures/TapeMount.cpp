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

#include "common/dataStructures/TapeMount.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::TapeMount::TapeMount() {  
  m_idSet = false;
  m_mountTypeSet = false;
  m_vidSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::TapeMount::~TapeMount() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::TapeMount::allFieldsSet() const {
  return m_idSet
      && m_mountTypeSet
      && m_vidSet;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void cta::dataStructures::TapeMount::setId(const uint64_t id) {
  m_id = id;
  m_idSet = true;
}

//------------------------------------------------------------------------------
// getId
//------------------------------------------------------------------------------
uint64_t cta::dataStructures::TapeMount::getId() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeMount have been set!");
  }
  return m_id;
}

//------------------------------------------------------------------------------
// setMountType
//------------------------------------------------------------------------------
void cta::dataStructures::TapeMount::setMountType(const cta::dataStructures::MountType &mountType) {
  m_mountType = mountType;
  m_mountTypeSet = true;
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::dataStructures::MountType cta::dataStructures::TapeMount::getMountType() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeMount have been set!");
  }
  return m_mountType;
}

//------------------------------------------------------------------------------
// setVid
//------------------------------------------------------------------------------
void cta::dataStructures::TapeMount::setVid(const std::string &vid) {
  m_vid = vid;
  m_vidSet = true;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::dataStructures::TapeMount::getVid() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the TapeMount have been set!");
  }
  return m_vid;
}
