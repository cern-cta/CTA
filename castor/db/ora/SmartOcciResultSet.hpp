/******************************************************************************
 *                      castor/db/ora/SmartOcciResultSet.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_DB_ORA_SMARTOCCIRESULTSET
#define CASTOR_DB_ORA_SMARTOCCIRESULTSET

#include "castor/exception/Exception.hpp"

#include "errno.h"
#include "occi.h"


namespace castor {
namespace db     {
namespace ora    {

/**
 * A simple smart-pointer that owns an oracle::occi::ResultSet.  When the smart
 * pointer goes out of scope, it will call statement->closeResultSet(resultSet).
 *
 * In order to close a result-set the statement from which it was obtained is
 * required.  Therefore the life-time of this smart-pointer must be shorter
 * than that of the statement from which the result-set was obtained.
 */
class SmartOcciResultSet {

public:

  /**
   * Constructor.
   *
   * This method throws an exception if either the statement or resultSet
   * arguments are NULL.
   *
   * @param statement The occi statement.
   * @param resultSet The open occi result-set to be owned by the smart pointer.
   */
  SmartOcciResultSet(
    oracle::occi::Statement *const statement,
    oracle::occi::ResultSet *const resultSet)
    throw(castor::exception::Exception);

  /**
   * Destructor.
   *
   * Calls statement->closeResultSet(resultSet) if the smart pointer has not
   * already done so.
   *
   * Please note that this method does not throw any exceptions because a
   * destructor should not an exception.
   */
  ~SmartOcciResultSet() throw();

  /**
   * Calls statement->closeResultSet(resultSet).
   *
   * This method throws a castor::exception::Exception if it is called more
   * than once and therefore the result-set is already closed.
   *
   * This method does not catch any oracle::Occi::SQLException's thrown by
   * statement->closeResultSet(resultSet) and they will therefore be passed
   * straight to the caller.
   */
  void close() throw(castor::exception::Exception, oracle::occi::SQLException);

  /**
   * Bypasses the smart pointer by returning the pointer to the owned
   * result-set.
   *
   * This method throws an exception if the owned result set has been closed.
   */
  oracle::occi::ResultSet *operator->() const
    throw(castor::exception::Exception);

  /**
   * Bypasses the smart pointer by returning the pointer to the owned
   * result-set.
   *
   * This method throws an exception if the owned result set has been closed.
   */
  oracle::occi::ResultSet * get() const
    throw(castor::exception::Exception);

private:

  /**
   * The statement of th ewoned result set.
   */
  oracle::occi::Statement *const m_statement;

  /**
   * The owned result set.
   */
  oracle::occi::ResultSet *const m_resultSet;

  /**
   * True if the owned result set has not been closed and is therefore still
   * open.
   */
  bool m_resultSetIsOpen;

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   */
  SmartOcciResultSet(const SmartOcciResultSet &obj) throw();

  /**
   * Private assignment-operator to prevent users from trying to assign one
   * object of this class to another.
   */
  SmartOcciResultSet &operator=(SmartOcciResultSet& obj) throw();

}; // class SmartOcciResultSet

} // namespace ora
} // namespace db
} // namespace castor

#endif // CASTOR_DB_ORA_SMARTOCCIRESULTSET
