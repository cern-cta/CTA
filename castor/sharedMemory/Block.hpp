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
 * @(#)$RCSfile: Block.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/10/26 13:33:30 $ $Author: felixehm $
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
       * @param size the size of the block to create when
       * creation is needed
       * @param address the process address to be used to
       * attach the memory block. If null, the system will
       * choose a suitable address
       */
      Block(key_t key, size_t size, const void* address)
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
       * initializes the Block.
       */
      void initialize()
        throw (castor::exception::Exception);

    private:

      /**
       * key of the block
       */
      key_t m_key;

      /**
       * size of the block
       */
      size_t m_size;

      // Convenience typedef
      typedef std::pair<void* const, size_t> SharedNode;
      typedef std::map<void*,
                       size_t,
                       std::less<void*>,
                       castor::sharedMemory::Allocator<SharedNode> >
      SharedMap;

      /**
       * Did this block create the shared memory segment ?
       */
      bool m_createdSharedMemory;

      /**
       * Did we initialize properly the Block ?
       */
      bool m_initialized;

      /**
       * Are we initializing the Block and if yes (non 0)
       * at which step are we (number of next node to
       * initialize in the allocation table)
       */
      unsigned int m_initializing;

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
       *     Head node of the Allocation Table
       *     First nodes of the Allocation Table
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
