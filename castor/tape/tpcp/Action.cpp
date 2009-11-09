/******************************************************************************
 *                 castor/tape/tpcp/Action.cpp
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
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/utils/utils.hpp"
#include <string.h>


//------------------------------------------------------------------------------
// read
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action castor::tape::tpcp::Action::read(
  castor::tape::tpcp::Action::READ, "READ");


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action castor::tape::tpcp::Action::write(
  castor::tape::tpcp::Action::WRITE, "WRITE");


//------------------------------------------------------------------------------
// dump
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action castor::tape::tpcp::Action::dump(
  castor::tape::tpcp::Action::DUMP, "DUMP");


//------------------------------------------------------------------------------
// verify
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action castor::tape::tpcp::Action::verify(
  castor::tape::tpcp::Action::VERIFY, "VERIFY");


//------------------------------------------------------------------------------
// s_objects
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action *castor::tape::tpcp::Action::s_objects[]
  = {
  &castor::tape::tpcp::Action::read,
  &castor::tape::tpcp::Action::write,
  &castor::tape::tpcp::Action::dump,
  &castor::tape::tpcp::Action::verify
};


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::Action::Action(const Value value,  const char *str)
  throw() :
  m_value(value),
  m_str(str) {
}


//------------------------------------------------------------------------------
// stringToObject
//------------------------------------------------------------------------------
const castor::tape::tpcp::Action &castor::tape::tpcp::Action::stringToObject(
  const char *const str) throw(castor::exception::InvalidArgument) {

  const int nbObjects = utils::arraySize(s_objects);

  // For each enumeration class object
  for(int i=0; i<nbObjects; i++) {
    // If the string to be tested matches the string value of the object
    if(strcmp(str, s_objects[i]->m_str) == 0) {

      // Return the object
      return *s_objects[i];
    }
  }

  castor::exception::InvalidArgument ex;
  ex.getMessage() << "Invalid action string: " << str;
  throw(ex);
}


//------------------------------------------------------------------------------
// writeValidStrings
//------------------------------------------------------------------------------
void castor::tape::tpcp::Action::writeValidStrings(std::ostream &os,
  const char *const separator) {

  const int nbActions = utils::arraySize(s_objects);

  // For each action
  for(int i=0; i<nbActions; i++) {
    // Add a separator if this is not the first string
    if(i > 0) {
      os << separator;
    }

    // Write the string of the actions to the output stream
    os << s_objects[i]->m_str;
  }
}


//------------------------------------------------------------------------------
// value
//------------------------------------------------------------------------------
castor::tape::tpcp::Action::Value castor::tape::tpcp::Action::value() const {
  return m_value;
}


//------------------------------------------------------------------------------
// str
//------------------------------------------------------------------------------
const char *castor::tape::tpcp::Action::str() const {
  return m_str;
}
