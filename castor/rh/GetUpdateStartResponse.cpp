/******************************************************************************
 *                      castor/rh/GetUpdateStartResponse.cpp
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
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/StartResponse.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::GetUpdateStartResponse::GetUpdateStartResponse() throw() :
  StartResponse(),
  m_emptyFile(false) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::rh::GetUpdateStartResponse::~GetUpdateStartResponse() throw() {
  m_sourcesVector.clear();
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::GetUpdateStartResponse::print(std::ostream& stream,
                                               std::string indent,
                                               castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# GetUpdateStartResponse #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Call print on the parent class(es)
  this->StartResponse::print(stream, indent, alreadyPrinted);
  // Output of all members
  stream << indent << "emptyFile : " << m_emptyFile << std::endl;
  alreadyPrinted.insert(this);
  {
    stream << indent << "Sources : " << std::endl;
    int i;
    std::vector<castor::stager::DiskCopyForRecall*>::const_iterator it;
    for (it = m_sourcesVector.begin(), i = 0;
         it != m_sourcesVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::GetUpdateStartResponse::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::rh::GetUpdateStartResponse::TYPE() {
  return OBJ_GetUpdateStartResponse;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::rh::GetUpdateStartResponse::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::rh::GetUpdateStartResponse::clone() {
  return new GetUpdateStartResponse(*this);
}

