/******************************************************************************
 *                      BaseMover.hpp
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
 * @(#)$RCSfile: BaseMover.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/26 14:54:54 $ $Author: waldron $
 *
 * Base class for the mover interface, implementing the default constructor and
 * stop method
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef BASE_MOVER_HPP
#define BASE_MOVER_HPP 1

// Include files
#include "castor/job/d2dtransfer/IMover.hpp"


namespace castor {

  namespace job {

    namespace d2dtransfer {

      /**
       * Base class for a mover
       */
      class BaseMover : public castor::job::d2dtransfer::IMover {

      public:
      
        /**
         * Default constructor
         */
        BaseMover();
      
        /**
         * Default destructor
         */
        virtual ~BaseMover() throw() {};
      
        /**
         * Returns the size of the source file represented in bytes
         */
        virtual u_signed64 sourceFileSize() {
          return m_sourceFileSize;
        }

        /**
         * Returns the number of bytes transferred from the client to the
         * source/destination
         */
        virtual u_signed64 bytesTransferred() {
          return m_bytesTransferred;
        }

      protected:

        /// Flag to indicate whether the mover has been asked to shutdown
        bool m_shutdown;

        /// The size in bytes of the source file
        u_signed64 m_sourceFileSize;

        /// The number of bytes transferred to the client/destination
        u_signed64 m_bytesTransferred;
      
      };
    
    } // End of name space diskcopy

  } // End of namespace job

} // End of namespace castor

#endif // BASE_MOVER_HPP
