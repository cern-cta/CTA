/******************************************************************************
 *                      castor/db/ora/SmartOcciResultSet.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/db/ora/SmartOcciResultSet.hpp"

#include <errno.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::db::ora::SmartOcciResultSet::SmartOcciResultSet(
  oracle::occi::Statement *const statement,
  oracle::occi::ResultSet *const resultSet)
   :
  m_statement(statement),
  m_resultSet(resultSet),
  m_resultSetIsOpen(true) {

  // Throw an exception if either of the input arguments are NULL
  if(statement == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() <<
      "Failed to create SmartOcciResultSet"
      ": statement argument to the constructor is NULL";

    throw ex;
  }
  if(resultSet == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() <<
      "Failed to create SmartOcciResultSet"
      ": resultSet argument to the constructor is NULL";

    throw ex;
  }
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::db::ora::SmartOcciResultSet::~SmartOcciResultSet() throw() {

  // Close the result-set if it is still open
  if(m_resultSetIsOpen) {
    try {
      close();
    }catch(...) {
      // Ignore any exceptions as we should not throw an exception in a
      // destructor
    }
  }
}


//-----------------------------------------------------------------------------
// close
//-----------------------------------------------------------------------------
void castor::db::ora::SmartOcciResultSet::close()
  throw(castor::exception::Exception, oracle::occi::SQLException) {

  // Throw an exception if the result-set is already closed
  if(!m_resultSetIsOpen) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Failed to close result-set"
      ": Result-set already closed";

    throw ex;
  }

  m_statement->closeResultSet(m_resultSet);
  m_resultSetIsOpen = false;
}


//-----------------------------------------------------------------------------
// SmartOcciResultSet arrow-operator
//-----------------------------------------------------------------------------
oracle::occi::ResultSet
  *castor::db::ora::SmartOcciResultSet::operator->() const
   {
  return get();
}

//-----------------------------------------------------------------------------
// SmartOcciResultSet get()
//-----------------------------------------------------------------------------
oracle::occi::ResultSet
  *castor::db::ora::SmartOcciResultSet::get() const
   {

  // Throw an exception if 
  if(!m_resultSetIsOpen) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Failed to dereference SmartOcciResultSet"
      ": Owned result-set has been closed";

    throw ex;
  }

  return m_resultSet;
}
