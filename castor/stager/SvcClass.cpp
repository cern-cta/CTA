/******************************************************************************
 *                      castor/stager/SvcClass.cpp
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
#include "castor/stager/DiskPool.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapePool.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::SvcClass::SvcClass() throw() :
  m_policy(""),
  m_nbDrives(0),
  m_name(""),
  m_defaultFileSize(0),
  m_id(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::SvcClass::~SvcClass() throw() {
  for (unsigned int i = 0; i < m_tapePoolsVector.size(); i++) {
    m_tapePoolsVector[i]->removeSvcClasses(this);
  }
  m_tapePoolsVector.clear();
  for (unsigned int i = 0; i < m_diskPoolsVector.size(); i++) {
    m_diskPoolsVector[i]->removeSvcClasses(this);
  }
  m_diskPoolsVector.clear();
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::SvcClass::print(std::ostream& stream,
                                     std::string indent,
                                     castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# SvcClass #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "policy : " << m_policy << std::endl;
  stream << indent << "nbDrives : " << m_nbDrives << std::endl;
  stream << indent << "name : " << m_name << std::endl;
  stream << indent << "defaultFileSize : " << m_defaultFileSize << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  {
    stream << indent << "TapePools : " << std::endl;
    int i;
    std::vector<TapePool*>::const_iterator it;
    for (it = m_tapePoolsVector.begin(), i = 0;
         it != m_tapePoolsVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
  {
    stream << indent << "DiskPools : " << std::endl;
    int i;
    std::vector<DiskPool*>::const_iterator it;
    for (it = m_diskPoolsVector.begin(), i = 0;
         it != m_diskPoolsVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::SvcClass::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::SvcClass::TYPE() {
  return OBJ_SvcClass;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::SvcClass::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::stager::SvcClass::clone() {
  return new SvcClass(*this);
}

