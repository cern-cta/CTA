/******************************************************************************
 *                      castor/stager/Cuuid.cpp
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
 * @(#)$RCSfile: Cuuid.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/10/01 14:26:21 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

// Include Files
#include "Cuuid.h"
#include "castor/Constants.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/Cuuid.hpp"
#include "castor/stager/Segment.hpp"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::Cuuid::Cuuid() throw() :
  m_content(),
  m_id(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::Cuuid::~Cuuid() throw() {
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Cuuid::print(std::ostream& stream,
                                  std::string indent,
                                  castor::ObjectSet& alreadyPrinted) const {
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "content : " << "("
         << m_content.time_low << ", "
         << m_content.time_mid << ", "
         << m_content.time_hi_and_version << ", "
         << m_content.clock_seq_hi_and_reserved << ", "
         << m_content.clock_seq_low << ", ("
         << m_content.node[0] << ","
         << m_content.node[1] << ","
         << m_content.node[2] << ","
         << m_content.node[3] << ","
         << m_content.node[4] << ","
         << m_content.node[5]
         << "))" << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Cuuid::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::Cuuid::TYPE() {
  return OBJ_Cuuid;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::stager::Cuuid::setId(unsigned long id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned long castor::stager::Cuuid::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::Cuuid::type() const {
  return TYPE();
}

