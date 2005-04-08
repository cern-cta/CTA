/******************************************************************************
 *                      castor/stager/FilesDeleted.cpp
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
#include "castor/stager/FilesDeleted.hpp"
#include "castor/stager/GCRemovedFile.hpp"
#include "castor/stager/Request.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::FilesDeleted::FilesDeleted() throw() :
  Request(),
  m_id(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::FilesDeleted::~FilesDeleted() throw() {
  for (unsigned int i = 0; i < m_filesVector.size(); i++) {
    m_filesVector[i]->setRequest(0);
    delete m_filesVector[i];
  }
  m_filesVector.clear();
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::FilesDeleted::print(std::ostream& stream,
                                         std::string indent,
                                         castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# FilesDeleted #]" << std::endl;
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
  {
    stream << indent << "Files : " << std::endl;
    int i;
    std::vector<GCRemovedFile*>::const_iterator it;
    for (it = m_filesVector.begin(), i = 0;
         it != m_filesVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::FilesDeleted::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::FilesDeleted::TYPE() {
  return OBJ_FilesDeleted;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::FilesDeleted::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::stager::FilesDeleted::clone() {
  return new FilesDeleted(*this);
}

