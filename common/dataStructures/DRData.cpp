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

#include "common/dataStructures/DRData.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::dataStructures::DRData::DRData() {  
  m_drBlobSet = false;
  m_drGroupSet = false;
  m_drInstanceSet = false;
  m_drOwnerSet = false;
  m_drPathSet = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::dataStructures::DRData::~DRData() throw() {
}

//------------------------------------------------------------------------------
// allFieldsSet
//------------------------------------------------------------------------------
bool cta::dataStructures::DRData::allFieldsSet() const {
  return m_drBlobSet
      && m_drGroupSet
      && m_drInstanceSet
      && m_drOwnerSet
      && m_drPathSet;
}

//------------------------------------------------------------------------------
// setDrBlob
//------------------------------------------------------------------------------
void cta::dataStructures::DRData::setDrBlob(const std::string &drBlob) {
  m_drBlob = drBlob;
  m_drBlobSet = true;
}

//------------------------------------------------------------------------------
// getDrBlob
//------------------------------------------------------------------------------
std::string cta::dataStructures::DRData::getDrBlob() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DRData have been set!");
  }
  return m_drBlob;
}

//------------------------------------------------------------------------------
// setDrGroup
//------------------------------------------------------------------------------
void cta::dataStructures::DRData::setDrGroup(const std::string &drGroup) {
  m_drGroup = drGroup;
  m_drGroupSet = true;
}

//------------------------------------------------------------------------------
// getDrGroup
//------------------------------------------------------------------------------
std::string cta::dataStructures::DRData::getDrGroup() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DRData have been set!");
  }
  return m_drGroup;
}

//------------------------------------------------------------------------------
// setDrInstance
//------------------------------------------------------------------------------
void cta::dataStructures::DRData::setDrInstance(const std::string &drInstance) {
  m_drInstance = drInstance;
  m_drInstanceSet = true;
}

//------------------------------------------------------------------------------
// getDrInstance
//------------------------------------------------------------------------------
std::string cta::dataStructures::DRData::getDrInstance() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DRData have been set!");
  }
  return m_drInstance;
}

//------------------------------------------------------------------------------
// setDrOwner
//------------------------------------------------------------------------------
void cta::dataStructures::DRData::setDrOwner(const std::string &drOwner) {
  m_drOwner = drOwner;
  m_drOwnerSet = true;
}

//------------------------------------------------------------------------------
// getDrOwner
//------------------------------------------------------------------------------
std::string cta::dataStructures::DRData::getDrOwner() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DRData have been set!");
  }
  return m_drOwner;
}

//------------------------------------------------------------------------------
// setDrPath
//------------------------------------------------------------------------------
void cta::dataStructures::DRData::setDrPath(const std::string &drPath) {
  m_drPath = drPath;
  m_drPathSet = true;
}

//------------------------------------------------------------------------------
// getDrPath
//------------------------------------------------------------------------------
std::string cta::dataStructures::DRData::getDrPath() const {
  if(!allFieldsSet()) {
    throw cta::exception::Exception(std::string(__FUNCTION__)+" Error: not all fields of the DRData have been set!");
  }
  return m_drPath;
}
