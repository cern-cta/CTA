/******************************************************************************
 *                      Exception.hpp
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
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef EXCEPTION_EXCEPTION_HPP
#define EXCEPTION_EXCEPTION_HPP 1

// Include Files
#include "castor/exception/Backtrace.hpp"
#include <serrno.h>
#include <sstream>
#include <exception>

namespace castor {

  namespace exception {

    /**
     * class Exception
     * A simple exception used for error handling in castor
     */
    class Exception: public std::exception {

    public:

      /**
       * Empty Constructor
       * @param serrno the serrno code of the corresponding C error
       */
      Exception(int se);

      /**
       * Copy Constructor
       */
      Exception(Exception& dbex);

      /**
       * Assignment operator
       */
      Exception& operator=(Exception &dbex);

      /**
       * Empty Destructor, explicitely non-throwing (needed for std::exception
       * inheritance)
       */
      virtual ~Exception() throw ();

      /**
       * Get the value of m_message
       * A message explaining why this exception was raised
       * @return the value of m_message
       */
      std::ostringstream& getMessage() {
        return m_message;
      }
      
      /**
       * Get the backtrace's contents
       * @return backtrace in a standard string.
       */
      std::string const backtrace() {
        return (std::string)m_backtrace;
      }
      
      /**
       * Updates the m_what member with a concatenation of the message and
       * the stack trace.
       * @return pointer to m_what's contents
       */
      virtual const char * what();

      /**
       * gets the serrno code of the corresponding C error
       */
      int code() const { return m_serrno; }

    private:
      /// A message explaining why this exception was raised
      std::ostringstream m_message;

      /**
       * The serrno code of the corresponding C error
       */
      int m_serrno;
      
      /**
       * Placeholder for the what result. It has to be a member
       * of the object, and not on the stack of the "what" function.
       */
      std::string m_what;
      
      /**
       * Backtrace object. Its constructor does the heavy lifting of
       * generating the backtrace.
       */
      Backtrace m_backtrace;

    };

  } // end of exception namespace

} // end of castor namespace


#endif // EXCEPTION_EXCEPTION_HPP
