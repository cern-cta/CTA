/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Backtrace.hpp"

#include <exception>
#include <sstream>

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
       * @param context optional context string added to the message
       * at initialisation time.
       */
      Exception(int se, std::string context="");

      /**
       * Empty Constructor with implicit serrno = SEINERNAL;
       * @param context optional context string added to the message
       * at initialisation time.
       */
      Exception(std::string context="");
      
      /**
       * Copy Constructor
       */
      Exception(const Exception& rhs);

      /**
       * Assignment operator
       */
      Exception& operator=(const Exception &rhs);

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
       * Get the value of m_message
       * A message explaining why this exception was raised
       * @return the value of m_message
       */
      const std::ostringstream& getMessage() const {
        return m_message;
      }
      
      /**
       * Get the value of m_message as a sting, for const-c orrectness
       * @return the value as a string.
       */
      std::string getMessageValue() const {
        return m_message.str();
      }
      
      /**
       * Get the backtrace's contents
       * @return backtrace in a standard string.
       */
      std::string const backtrace() const {
        return (std::string)m_backtrace;
      }
      
      /**
       * Updates the m_what member with a concatenation of the message and
       * the stack trace.
       * @return pointer to m_what's contents
       */
      virtual const char * what() const throw ();

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
      mutable std::string m_what;
      
    protected:  
      void setWhat(const std::string &w);
      
      /**
       * Backtrace object. Its constructor does the heavy lifting of
       * generating the backtrace.
       */
      Backtrace m_backtrace;

    };

  } // end of exception namespace

} // end of castor namespace


