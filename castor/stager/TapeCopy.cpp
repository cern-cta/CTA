/******************************************************************************
 *                      castor/stager/TapeCopy.cpp
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
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::TapeCopy::TapeCopy() throw() :
  m_copyNb(0),
  m_id(),
  m_castorFile(0),
  m_status(TapeCopyStatusCodes(0)) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::TapeCopy::~TapeCopy() throw() {
  for (unsigned int i = 0; i < m_streamVector.size(); i++) {
    m_streamVector[i]->removeTapeCopy(this);
  }
  m_streamVector.clear();
  for (unsigned int i = 0; i < m_segmentsVector.size(); i++) {
    m_segmentsVector[i]->setCopy(0);
    delete m_segmentsVector[i];
  }
  m_segmentsVector.clear();
  if (0 != m_castorFile) {
    m_castorFile->removeTapeCopies(this);
  }
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::TapeCopy::print(std::ostream& stream,
                                     std::string indent,
                                     castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# TapeCopy #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "copyNb : " << m_copyNb << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  {
    stream << indent << "Stream : " << std::endl;
    int i;
    std::vector<Stream*>::const_iterator it;
    for (it = m_streamVector.begin(), i = 0;
         it != m_streamVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
  {
    stream << indent << "Segments : " << std::endl;
    int i;
    std::vector<Segment*>::const_iterator it;
    for (it = m_segmentsVector.begin(), i = 0;
         it != m_segmentsVector.end();
         it++, i++) {
      stream << indent << "  " << i << " :" << std::endl;
      (*it)->print(stream, indent + "    ", alreadyPrinted);
    }
  }
  stream << indent << "CastorFile : " << std::endl;
  if (0 != m_castorFile) {
    m_castorFile->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "status : " << TapeCopyStatusCodesStrings[m_status] << std::endl;
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::TapeCopy::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::TapeCopy::TYPE() {
  return OBJ_TapeCopy;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::stager::TapeCopy::setId(u_signed64 id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
u_signed64 castor::stager::TapeCopy::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::TapeCopy::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::stager::TapeCopy::clone() {
  return new TapeCopy(*this);
}

