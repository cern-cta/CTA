/******************************************************************************
 *                      castor/rh/StartResponse.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::StartResponse::StartResponse() throw() :
  Response(),
  m_id(0),
  m_diskCopy(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::rh::StartResponse::~StartResponse() throw() {
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::StartResponse::print(std::ostream& stream,
                                      std::string indent,
                                      castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# StartResponse #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Call print on the parent class(es)
  this->Response::print(stream, indent, alreadyPrinted);
  // Output of all members
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "DiskCopy : " << std::endl;
  if (0 != m_diskCopy) {
    m_diskCopy->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::StartResponse::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::rh::StartResponse::TYPE() {
  return OBJ_StartResponse;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::rh::StartResponse::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::rh::StartResponse::clone() {
  return new StartResponse(*this);
}

