/******************************************************************************
 *                      castor/MessageAck.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/MessageAck.hpp"
#include "castor/ObjectSet.hpp"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::MessageAck::MessageAck() throw() :
  m_status(false),
  m_errorCode(0),
  m_errorMessage(""),
  m_id(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::MessageAck::~MessageAck() throw() {
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::MessageAck::print(std::ostream& stream,
                               std::string indent,
                               castor::ObjectSet& alreadyPrinted) const
  throw (){
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "status : " << m_status << std::endl;
  stream << indent << "errorCode : " << m_errorCode << std::endl;
  stream << indent << "errorMessage : " << m_errorMessage << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::MessageAck::print() const throw() {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::MessageAck::TYPE() {
  return OBJ_MessageAck;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::MessageAck::setId(unsigned long id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned long castor::MessageAck::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::MessageAck::type() const {
  return TYPE();
}

