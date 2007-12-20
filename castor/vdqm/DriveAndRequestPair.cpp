/******************************************************************************
 *                      DriveAndRequestPair.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author castor dev team
 *****************************************************************************/
 
#include "castor/Constants.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/vdqm/DriveAndRequestPair.hpp"
#include "castor/vdqm/TapeDrive.hpp"
#include "castor/vdqm/TapeRequest.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveAndRequestPair::DriveAndRequestPair() throw() :
  m_id(0),
  m_tapeDrive(0),
  m_tapeRequest(0) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::vdqm::DriveAndRequestPair::~DriveAndRequestPair() throw() {
  if(0 != m_tapeDrive) {
    delete m_tapeDrive;
    m_tapeDrive = 0;
  }

  if(0 != m_tapeRequest) {
    delete m_tapeRequest;
    m_tapeRequest = 0;
  }
}


//-----------------------------------------------------------------------------
// setId
//-----------------------------------------------------------------------------
void castor::vdqm::DriveAndRequestPair::setId(u_signed64 id) {
  m_id = id;
}


//-----------------------------------------------------------------------------
// id
//-----------------------------------------------------------------------------
u_signed64 castor::vdqm::DriveAndRequestPair::id() const {
  return m_id;
}


//-----------------------------------------------------------------------------
// type
//-----------------------------------------------------------------------------
int castor::vdqm::DriveAndRequestPair::type() const {
  return OBJ_DriveAndRequestPair;
}


//-----------------------------------------------------------------------------
// clone
//-----------------------------------------------------------------------------
castor::IObject* castor::vdqm::DriveAndRequestPair::clone() {
  return new DriveAndRequestPair(*this);
}


//-----------------------------------------------------------------------------
// print
//-----------------------------------------------------------------------------
void castor::vdqm::DriveAndRequestPair::print(std::ostream& stream,
  std::string indent, castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# DriveAndRequestPair #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "TapeDrive : " << std::endl;
  if (0 != m_tapeDrive) {
    m_tapeDrive->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "TapeRequest: " << std::endl;
  if (0 != m_tapeRequest) {
    m_tapeRequest->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
}


//-----------------------------------------------------------------------------
// print
//-----------------------------------------------------------------------------
void castor::vdqm::DriveAndRequestPair::print() const {
  castor::ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}


//-----------------------------------------------------------------------------
// setTapeDrive
//-----------------------------------------------------------------------------
void castor::vdqm::DriveAndRequestPair::setTapeDrive(
  castor::vdqm::TapeDrive *tapeDrive) throw() {
  m_tapeDrive = tapeDrive;
}


//-----------------------------------------------------------------------------
// tapeDrive
//-----------------------------------------------------------------------------
castor::vdqm::TapeDrive *castor::vdqm::DriveAndRequestPair::tapeDrive() const
  throw() {
  return m_tapeDrive;
}


//-----------------------------------------------------------------------------
// setTapeRequest
//-----------------------------------------------------------------------------
void castor::vdqm::DriveAndRequestPair::setTapeRequest(
  castor::vdqm::TapeRequest* tapeRequest) throw() {
  m_tapeRequest = tapeRequest;
}


//-----------------------------------------------------------------------------
// tapeRequest
//-----------------------------------------------------------------------------
castor::vdqm::TapeRequest* castor::vdqm::DriveAndRequestPair::tapeRequest()
  const throw() {
  return m_tapeRequest;
}
