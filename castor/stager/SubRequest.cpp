/******************************************************************************
 *                      castor/stager/SubRequest.cpp
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
#include "castor/ObjectSet.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::SubRequest::SubRequest() throw() :
  m_retryCounter(0),
  m_fileName(""),
  m_protocol(""),
  m_xsize(),
  m_priority(0),
  m_subreqId(""),
  m_id(),
  m_diskcopy(0),
  m_castorFile(0),
  m_parent(0),
  m_status(SubRequestStatusCodes(0)),
  m_request(0) {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::stager::SubRequest::~SubRequest() throw() {
  if (0 != m_parent) {
    m_parent->removeChild(this);
  }
  if (0 != m_request) {
    m_request->removeSubRequests(this);
  }
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::SubRequest::print(std::ostream& stream,
                                       std::string indent,
                                       castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# SubRequest #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "retryCounter : " << m_retryCounter << std::endl;
  stream << indent << "fileName : " << m_fileName << std::endl;
  stream << indent << "protocol : " << m_protocol << std::endl;
  stream << indent << "xsize : " << m_xsize << std::endl;
  stream << indent << "priority : " << m_priority << std::endl;
  stream << indent << "subreqId : " << m_subreqId << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
  stream << indent << "Diskcopy : " << std::endl;
  if (0 != m_diskcopy) {
    m_diskcopy->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "CastorFile : " << std::endl;
  if (0 != m_castorFile) {
    m_castorFile->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "Parent : " << std::endl;
  if (0 != m_parent) {
    m_parent->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
  stream << indent << "status : " << m_status << std::endl;
  stream << indent << "Request : " << std::endl;
  if (0 != m_request) {
    m_request->print(stream, indent + "  ", alreadyPrinted);
  } else {
    stream << indent << "  null" << std::endl;
  }
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::SubRequest::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::SubRequest::TYPE() {
  return OBJ_SubRequest;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::stager::SubRequest::setId(u_signed64 id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
u_signed64 castor::stager::SubRequest::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::SubRequest::type() const {
  return TYPE();
}

