/******************************************************************************
 *                      Block.cpp
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
 * @(#)$RCSfile: Block.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2006/11/03 11:08:39 $ $Author: sponcec3 $
 *
 * A block of shared memory with incorporated memory allocation
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_BLOCK_HPP
#define SHAREDMEMORY_BLOCK_HPP 1

#include <map>
#include "castor/sharedMemory/BasicBlock.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    // Convenience typedef
    typedef std::pair<void* const, size_t> SharedNode;

    /**
     * a class dealing with a Block of Shared memory.
     * The Block contains its own table of free and used areas
     * so that it's self contained.
     * The mapping of the memory of the Block is the following :
     * \verbatim
     *   Begin of Block
     *     First node of the Allocation Table
     *     Allocation Table
     *     Available memory
     *   End of Block
     * \endverbatim
     * @param A an allocator name that should be used for internal
     * memory allocation. Node that it must allocate
     * Block::SharedNode objects
     */
    template <typename A>
    class Block : public BasicBlock {

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

      // Convenience typedef
      typedef typename std::map<void*,
                                size_t,
                                std::less<void*>,
                                A> SharedMap;

      /**
       * Are we initializing the Block ? If yes, we cannot yet
       * use the free regions table.
       */
      bool m_initializing;

      /**
       * map of free regions.
       */
      SharedMap* m_freeRegions;

    }; // end class Block

  } // namespace sharedMemory

} // namespace castor


////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include "castor/sharedMemory/Allocator.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/dlf/Dlf.hpp"
#include <sys/ipc.h>
#include <sys/shm.h>
#include <errno.h>
#include <Cmutex.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
template <typename A>
castor::sharedMemory::Block<A>::Block (BlockKey& key)
  throw (castor::exception::Exception) :
  BasicBlock(key),
  m_initializing(false) {
    // Check that the given size is big enough
    size_t minimumSize =
      2*sizeof(std::_Rb_tree_node<SharedNode>) +
      sizeof(SharedMap);
    if (minimumSize > m_key.size()) {
      // "Not enough space for headers in shared memory. Giving up"
      castor::dlf::Param initParams[] =
        {castor::dlf::Param("Allocated size", m_key.size()),
         castor::dlf::Param("Requested size", minimumSize)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 4, 0, 0);
      castor::exception::Internal e;
      e.getMessage() << "Not enough space for header in shared memory. Giving up.";
      throw e;
    }
    // we need to assure that we create the shared memory only once
    // so we serialize all the Block creation
    Cmutex_lock(&m_sharedMemoryBlock, -1);
    // Keep track of whether we created the shared memory block
    bool createdSharedMemory = false;
    // Try to get the identifier of the shared memory
    int shmid = shmget(m_key.key(), m_key.size(), 0666);
    if (-1 == shmid) {
      if (ENOENT != errno) {
        // "Unable to get shared memory id. Giving up"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Error Message", strerror(errno))};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 0, 1, initParams);
        castor::exception::Internal e;
        e.getMessage() << "Unable to get shared memory id. Giving up.";
        throw e;
      }
      // Try to create the shared memory
      shmid = shmget(m_key.key(), m_key.size(), IPC_CREAT | 0666);
      if (-1 == shmid) {
        // "Unable to create shared memory. Giving up"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Error Message", strerror(errno))};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1, 1, initParams);
        castor::exception::Internal e;
        e.getMessage() << "Unable to create shared memory. Giving up.";
        throw e;
      }
      createdSharedMemory = true;
      // "Created the shared memory."
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 0, 0);
    }
    // Check whether the block is already attached
    std::map<int, void*>::const_iterator it =
      s_attachedBlocks.find(shmid);
    if (it == s_attachedBlocks.end()) {
      // Attach the shared memory
      m_sharedMemoryBlock = shmat(shmid, m_key.address(), SHM_REMAP);
      if (-1 == (int)m_sharedMemoryBlock) {
        // "Unable to get pointer to shared memory. Giving up"
        castor::dlf::Param initParams[] =
          {castor::dlf::Param("Error Message", strerror(errno))};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 1, initParams);
        castor::exception::Internal e;
        e.getMessage() << "Unable to get pointer to shared memory. Giving up.";
        throw e;
      }
      s_attachedBlocks[shmid] = m_sharedMemoryBlock;
    } else {
      m_sharedMemoryBlock = it->second;
    }
    // start of memory initialization
    m_initializing = true;
    // now create/extract the map of allocated regions
    size_t offset = sizeof(std::_Rb_tree_node<SharedNode>);
    void* m_freeRegionsRaw = (void*)((char*)m_sharedMemoryBlock + offset);
    if (createdSharedMemory) {
      // create the free region map
      m_freeRegions = new(m_freeRegionsRaw) SharedMap();
      offset += sizeof(SharedMap);
      // note that malloc will be called here since a new cell
      // has to be created in the m_freeRegions container
      // However, since m_initializing is true, it won't allocate memory
      // but used the reserved node
      (*m_freeRegions)[(void*)((char*)m_sharedMemoryBlock + offset)] =
        m_key.size() - offset;
    } else {
      m_freeRegions = (SharedMap*) (m_freeRegionsRaw);
    }
    // End of memory initialization
    m_initializing = false;
    // Now we can release the lock
    Cmutex_unlock(&m_sharedMemoryBlock);
  }

//------------------------------------------------------------------------------
// malloc
//------------------------------------------------------------------------------
template <typename A>
void* castor::sharedMemory::Block<A>::malloc(size_t nbBytes)
  throw (castor::exception::Exception) {
  if (m_initializing) {
    // Very special case where we are "recursively" called
    // for the creation of the first node of the map Btree.
    // We return straight the space dedicated to it, which
    // is located at the very beginning of the block
    void* res = (void*) ((char*)m_sharedMemoryBlock);
    return res;
  }
  // loop over free regions to find a suitable one
  for (typename SharedMap::iterator it =
         m_freeRegions->begin();
       it != m_freeRegions->end();
       it++) {
    // Take the first one large enough
    if (it->second >= nbBytes) {
      // read everything before we screw the iterator
      void* pointer = it->first;
      // remove from the free regions
      if (it->second == nbBytes) {
        // the perfect match
        m_freeRegions->erase(it);
      } else {
        // Let's reduce the region
        it->second -= nbBytes;
        pointer = (void*)((char*)pointer + it->second);
      }
      return pointer;
    }
  }
  // none found, i.e. out of memory
  castor::exception::OutOfMemory e;
  e.getMessage() << "Out of Shared memory. Cannot allocate "
                 << nbBytes << " bytes";
  throw e;
}

//------------------------------------------------------------------------------
// free
//------------------------------------------------------------------------------
template <typename A>
void castor::sharedMemory::Block<A>::free
(void* pointer, size_t nbBytes)
  throw (castor::exception::Exception) {
  // Update free space
  typename SharedMap::iterator it = m_freeRegions->begin();
  // First find the surrounding free regions (if any)
  std::pair<void*, size_t> previousRegion(0, 0);
  it = m_freeRegions->begin();
  while ((it != m_freeRegions->end()) && // end of iteration
         (it->first <= pointer)) { // found the right one
    previousRegion = *it;
    it++;
  }
  // Merge with the previous one ?
  bool merged = false;
  size_t size = nbBytes;
  if (previousRegion.first > 0 &&
      ((void*)((char*)previousRegion.first +
               previousRegion.second)) == pointer) {
    pointer = previousRegion.first;
    size += previousRegion.second;
    (*m_freeRegions)[pointer] = size;
    merged = true;
  }
  // Merge with the next one ?
  if (it != m_freeRegions->end() &&
      it->first == (void*)((char*)pointer + size)) {
    (*m_freeRegions)[pointer] = size + it->second;
    m_freeRegions->erase(it);
    merged = true;
  }
  if (!merged) {
    // No merge, create a new free Region
    // Note that this will trigger an allocation of memory
    // for the cell of the new free region within the
    // m_freeRegions container. Thus malloc will be called
    // at some point inside free !
    (*m_freeRegions)[pointer] = size;
  }
}

#endif // SHAREDMEMORY_BLOCK_HPP
