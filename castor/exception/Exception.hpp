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
 * @(#)$RCSfile: Exception.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/19 16:37:21 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef EXCEPTION_EXCEPTION_HPP
#define EXCEPTION_EXCEPTION_HPP 1

// Include Files
#include <serrno.h>
#include <sstream>

namespace castor {

  namespace exception {

    /**
     * class Exception
     * A simple exception used for error handling in castor
     */
    class Exception {

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
       * Empty Destructor
       */
      virtual ~Exception();

      /**
       * Get the value of m_message
       * A message explaining why this exception was raised
       * @return the value of m_message
       */
      std::ostringstream& getMessage() {
        return m_message;
      }

      /**
       * gets the serrno code of the corresponding C error
       */
      const int code() const { return m_serrno; }

    private:
      /// A message explaining why this exception was raised
      std::ostringstream m_message;

      /**
       * The serrno code of the corresponding C error
       */
      int m_serrno;

    };

  } // end of exception namespace

} // end of castor namespace


#endif // EXCEPTION_EXCEPTION_HPP
