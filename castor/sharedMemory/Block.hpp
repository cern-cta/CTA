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
 * @(#)$RCSfile: Block.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/09/25 09:21:22 $ $Author: sponcec3 $
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

  // Forward Declaration
  namespace rmmaster { 
    class ClusterStatus;
  }

  namespace sharedMemory {

    /**
     * a class holding a table of free and used areas in the
     * shared memory
     */
    class Block {

    public:

      /**
       * Constructor
       * Initiates a block of shared memory
       * @param key the key for this block
       * @param size the size of the block to create when
       * creation is needed
       */
      Block(key_t key, size_t size)
        throw (castor::exception::Exception);

      /**
       * get a pointer to the cluster status
       */
      castor::rmmaster::ClusterStatus* clusterStatus() throw();

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

      /**
       * prints the status of the allocation table
       * @param out the stream where to print
       */
      void print(std::iostream& out) throw();

    private:
      
      /**
       * key of the block (not used)
       */
      key_t m_key;

      /**
       * size of the block (not used)
       */
      size_t m_size;

      // Convenience typedef
      typedef std::map<void*,
                       size_t,
                       std::less<void*>,
                       castor::sharedMemory::Allocator<std::pair<void* const, size_t> > >
      SharedMap;

      /**
       * map of free regions.
       */
      SharedMap* m_freeRegions;

      /**
       * ClusterStatus object
       */
      castor::rmmaster::ClusterStatus* m_clusterStatus;

    }; // end class Block

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_BLOCK_HPP
