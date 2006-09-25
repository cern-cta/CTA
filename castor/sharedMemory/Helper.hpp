/******************************************************************************
 *                      Helper.hpp
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
 * @(#)$RCSfile: Helper.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/09/25 09:21:23 $ $Author: sponcec3 $
 *
 * A singleton for the shared memory usage
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_HELPER_HPP 
#define SHAREDMEMORY_HELPER_HPP 1

#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    // Forward declaration
    class Block;

    /**
     * A singleton class dedicated to the handling of
     * shared memory and using the internal classes.
     */
    class Helper {

    public:

      /**
       * gets a pointer to THE shared memory block
       */
      static Block* getBlock() throw (castor::exception::Exception);

    private:

      /**
       * the shared memory block address, when attached
       */
      static void* s_smBlockAddress;

    };

  }  // namespace sharedMemory

} // namespace castor



#endif // SHAREDMEMORY_HELPER_HPP
