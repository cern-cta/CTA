/******************************************************************************
 *                      castor/stager/FileResult.cpp
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
 * Description of the result of the processing of a file within a bulk request
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "castor/Constants.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/FileResult.hpp"
#include <iostream>
#include <string>

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::stager::FileResult::FileResult() throw() :
  m_fileId(0),
  m_nsHost(""),
  m_errorCode(0),
  m_errorMessage(""),
  m_id(0) {
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::FileResult::print(std::ostream& stream,
                                       std::string indent,
                                       castor::ObjectSet& alreadyPrinted) const {
  stream << indent << "[# FileResult #]" << std::endl;
  if (alreadyPrinted.find(this) != alreadyPrinted.end()) {
    // Circular dependency, this object was already printed
    stream << indent << "Back pointer, see above" << std::endl;
    return;
  }
  // Output of all members
  stream << indent << "fileId : " << m_fileId << std::endl;
  stream << indent << "nsHost : " << m_nsHost << std::endl;
  stream << indent << "errorCode : " << m_errorCode << std::endl;
  stream << indent << "errorMessage : " << m_errorMessage << std::endl;
  alreadyPrinted.insert(this);
}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::stager::FileResult::print() const {
  castor::ObjectSet alreadyPrinted;
  print(std::cout, "", alreadyPrinted);
}

//------------------------------------------------------------------------------
// TYPE
//------------------------------------------------------------------------------
int castor::stager::FileResult::TYPE() {
  return OBJ_FileResult;
}

//------------------------------------------------------------------------------
// type
//------------------------------------------------------------------------------
int castor::stager::FileResult::type() const {
  return TYPE();
}

//------------------------------------------------------------------------------
// clone
//------------------------------------------------------------------------------
castor::IObject* castor::stager::FileResult::clone() {
  return new FileResult(*this);
}

