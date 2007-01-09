/******************************************************************************
 *                      Block.hpp
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
 * @(#)$RCSfile: IBlock.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/01/09 16:18:35 $ $Author: sponcec3 $
 *
 * abstract interface for a block of shared memory
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_IBLOCK_HPP 
#define SHAREDMEMORY_IBLOCK_HPP 1

#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    /**
     * an abstract interface for a Block of Shared memory.
     */
    class IBlock {

    public:

      /**
       * virtual destructor
       */
      virtual ~IBlock() {};

      /**
       * allocates a new chunk of memory
       * @param nbBytes the number of bytes needed
       */
      virtual void* malloc(size_t nbBytes)
        throw (castor::exception::Exception) = 0;

      /**
       * deallocate some memory
       * @param pointer a pointer to the space to deallocate
       * @param nbBytes the number of bytes freed
       */
      virtual void free(void* pointer, size_t nbBytes)
        throw (castor::exception::Exception) = 0;

    }; // end class IBlock

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_IBLOCK_HPP
