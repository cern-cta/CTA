/******************************************************************************
 *                      castor/stager/UpdateRepRequest.cpp
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
 * @(#)$RCSfile: UpdateRepRequest.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/30 11:28:00 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/UpdateRepRequest.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::UpdateRepRequest::UpdateRepRequest() throw() :
  Request(),
  m_id(),
  m_object(0),
  m_address(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::UpdateRepRequest::~UpdateRepRequest() throw() {
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::UpdateRepRequest::print(std::ostream& stream,
                                             std::string indent,
                                             castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# UpdateRepRequest #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Call print on the parent class(es)
  this->Request::print(stream, indent, alreadyPrinted);
  // Output of all members
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "Object : " << std::endl;
  if (0 != m_object) {
    m_object->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "Address : " << std::endl;
  if (0 != m_address) {
    m_address->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::UpdateRepRequest::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::UpdateRepRequest::TYPE() {
  return OBJ_UpdateRepRequest;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::UpdateRepRequest::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::stager::UpdateRepRequest::clone() {
  return new UpdateRepRequest(*this);
}

