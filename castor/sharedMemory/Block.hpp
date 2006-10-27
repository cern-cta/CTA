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
 * @(#)$RCSfile: Block.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2006/10/27 15:29:32 $ $Author: sponcec3 $
 *
 * a block of shared memory with incorporated memory
 * allocation
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_BLOCK_HPP 
#define SHAREDMEMORY_BLOCK_HPP 1

#include <map>
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    /**
     * a class dealing with a Block of Shared memory.
     * The Block contains its own table of free and used areas
     * so that it's self contained.
     */
    class Block {

    public:

      /**
       * Constructor
       * Initiates a block of shared memory.
       * @param key the key for this block
       */
      Block(BlockKey& key)
        throw (castor::exception::Exception);

    public:

      /**
       * allocates a new chunk of memory
       * @param nbBytes the number of bytes needed
       */
      void* malloc(size_t nbBytes)
        throw (castor::exception::Exception);

      /**
       * deallocate some memory
       * @param pointer a pointer to the space to deallocate
       * @param nbBytes the number of bytes freed
       */
      void free(void* pointer, size_t nbBytes)
        throw (castor::exception::Exception);

    private:

      /**
       * A hook to be used by subclasses to initialize
       * specific things, like objects located in the
       * shared memory. Default implementation is empty
       * @param address the address where to put objects
       * @param isSMCreated tells whether the shared memory was
       * created by this call, or only attached
       * @return the address immediately after the last
       * object created by this method
       */
      virtual void* initialize(void* address, bool isSMCreated)
        throw (castor::exception::Exception) {};

    private:

      /**
       * key of the block. This includes the actual key,
       * the size of the block and the desired mapping address
       * (may be null)
       */
      BlockKey m_key;

      // Convenience typedef
      typedef std::pair<void* const, size_t> SharedNode;
      typedef std::map<void*,
                       size_t,
                       std::less<void*>,
                       castor::sharedMemory::Allocator<SharedNode> >
      SharedMap;

      /**
       * Are we initializing the Block ? If yes, we cannot yet
       * use the free regions table.
       */
      bool m_initializing;

      /**
       * List of already attached memory blocks
       */
      static std::map<int, void*> s_attachedBlocks;

      /**
       * Pointer to the raw shared memory block
       * The mapping of the memory is as follows.
       * of the Block is the following :
       * \verbatim
       *   Begin of Block
       *     First node of the Allocation Table
       *     [Custom objects created by subclasses via initialize]
       *     Allocation Table
       *     Available memory
       *   End of Block
       * \endverbatim
       */
      void* m_sharedMemoryBlock;

      /**
       * map of free regions.
       */
      SharedMap* m_freeRegions;

    }; // end class Block

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_BLOCK_HPP
