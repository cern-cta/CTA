/******************************************************************************
 *                      castor/stager/Tape.cpp
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
#include "castor/ObjectSet.hpp"
#include "castor/stager/Segment.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/Tape.hpp"
#include "osdep.h"
#include <iostream>
#include <string>
#include <vector>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::Tape::Tape() throw() :
  m_vid(""),
  m_side(0),
  m_tpmode(0),
  m_errMsgTxt(""),
  m_errorCode(0),
  m_severity(0),
  m_vwAddress(""),
  m_id(),
  m_stream(0),
  m_status(TapeStatusCodes(0)) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::Tape::~Tape() throw() {
  if (0 != m_stream) {
    m_stream->setTape(0);
  }
  for (unsigned int i = 0; i < m_segmentsVector.size(); i++) {
    m_segmentsVector[i]->setTape(0);
    delete m_segmentsVector[i];
  }
  m_segmentsVector.clear();
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Tape::print(std::ostream& stream,
                                 std::string indent,
                                 castor::ObjectSet& alreadyPrinted) const {
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "vid : " << m_vid << std::endl;
  stream << indent << "side : " << m_side << std::endl;
  stream << indent << "tpmode : " << m_tpmode << std::endl;
  stream << indent << "errMsgTxt : " << m_errMsgTxt << std::endl;
  stream << indent << "errorCode : " << m_errorCode << std::endl;
  stream << indent << "severity : " << m_severity << std::endl;
  stream << indent << "vwAddress : " << m_vwAddress << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "Stream : " << std::endl;
  if (0 != m_stream) {
    m_stream->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
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
  stream << indent << "status : " << m_status << std::endl;
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::Tape::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::Tape::TYPE() {
  return OBJ_Tape;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::stager::Tape::setId(u_signed64 id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
u_signed64 castor::stager::Tape::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::Tape::type() const {
  return TYPE();
}

