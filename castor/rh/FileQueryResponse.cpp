/******************************************************************************
 *                      castor/rh/FileQueryResponse.cpp
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
#include "castor/rh/FileQueryResponse.hpp"
#include "castor/rh/Response.hpp"
#include "osdep.h"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::rh::FileQueryResponse::FileQueryResponse() throw() :
  Response(),
  m_fileName(""),
  m_fileId(),
  m_status(0),
  m_size(),
  m_poolName(""),
  m_creationTime(),
  m_accessTime(),
  m_nbAccesses(0),
  m_id() {
};

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::rh::FileQueryResponse::~FileQueryResponse() throw() {
};

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::FileQueryResponse::print(std::ostream& stream,
                                          std::string indent,
                                          castor::ObjectSet& alreadyPrinted) const {
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Call print on the parent class(es)
  this->Response::print(stream, indent, alreadyPrinted);
  // Output of all members
  stream << indent << "fileName : " << m_fileName << std::endl;
  stream << indent << "fileId : " << m_fileId << std::endl;
  stream << indent << "status : " << m_status << std::endl;
  stream << indent << "size : " << m_size << std::endl;
  stream << indent << "poolName : " << m_poolName << std::endl;
  stream << indent << "creationTime : " << m_creationTime << std::endl;
  stream << indent << "accessTime : " << m_accessTime << std::endl;
  stream << indent << "nbAccesses : " << m_nbAccesses << std::endl;
  stream << indent << "id : " << m_id << std::endl;
  alreadyPrinted.insert(this);
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rh::FileQueryResponse::print() const {
  ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::rh::FileQueryResponse::TYPE() {
  return OBJ_FileQueryResponse;
}

//------------------------------------------------------------------------------
// setId
//------------------------------------------------------------------------------
void castor::rh::FileQueryResponse::setId(u_signed64 id) {
  m_id = id;
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
u_signed64 castor::rh::FileQueryResponse::id() const {
  return m_id;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::rh::FileQueryResponse::type() const {
  return TYPE();
}

