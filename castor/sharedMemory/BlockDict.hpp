/******************************************************************************
 *                      BlockDict.hpp
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
 * @(#)$RCSfile: BlockDict.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2007/04/13 15:25:07 $ $Author: itglp $
 *
 * A static dictionnary of blocks, referenced by their
 * BlockKey
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_BLOCKDICT_HPP
#define SHAREDMEMORY_BLOCKDICT_HPP 1

#include <map>
#include "castor/sharedMemory/BlockKey.hpp"

namespace castor {

  namespace sharedMemory {

    // Forward declaration
    class LocalBlock;

    /**
     * Just a container for the dictionnary,
     */
    class BlockDict {

    public:

      /**
       * retrieves a block by key. Does not create any new block
       * @return a pointer to a LocalBlock or 0 if not found.
       * NOte that this pointer should not be deallocated by the
       * caller.
       */
      static LocalBlock* getLocalBlock(BlockKey &key);

      /**
       * retrieves any kind of block by key
       * In case the block does not yet exist and create is true,
       * creates it.
       * This method is to be used with care since it does
       * a static cast of the block found for key into the
       * desired type, without any further check
       * @param key the BlockKey object to identify this block
       * @param create when true, the block is created if doesn't exist.
       * The returned value tells whether the block was created or not.
       * @return the instantiated block, or NULL if create was false
       * and no shared memory area was found.
       */
      template<typename T>
      static T* getBlock(BlockKey &key, bool& create);

      /**
       * add a block
       */
      static bool insertBlock
      (BlockKey &key,
       BasicBlock* block,
       void* (*mallocMethod)(BasicBlock* obj, size_t nbBytes),
       void (*freeMethod)(BasicBlock* obj, void* pointer, size_t nbBytes));

    private:

      /**
       * create a block
       * @param pointer is filled with a pointer to the raw memory of the block
       * @param create whether the block has to be really created or only attached
       * (it is returned true when it has been really created)
       */
      static void createBlock(BlockKey &key, void** pointer, bool& create);

    private:

      /**
       * the dictionnary of existing blocks
       */
      static std::map<BlockKey, LocalBlock*> s_blockDict;

    }; // class BlockDict

  } // namespace sharedMemory

} // namespace castor

////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include "castor/exception/Internal.hpp"
#include "castor/sharedMemory/LocalBlock.hpp"
#include "castor/dlf/Dlf.hpp"
#include <Cmutex.h>

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
template <typename T>
T* castor::sharedMemory::BlockDict::getBlock
(castor::sharedMemory::BlockKey &key, bool& create) {
  LocalBlock* block = getLocalBlock(key);
  if (0 != block) {
    return (T*)block->block();
  }
  // we need to assure that we create the shared memory only once
  // so we serialize all the Block creation
  Cmutex_lock(&s_blockDict, -1);
  // create the block
  void* sharedMemoryBlock;
  createBlock(key, &sharedMemoryBlock, create);
  // Now let's really create/retrieve the Block object inside the shared memory
  T* tblock = 0;
  if (create) {
    // Object creation
    // Note that it will register itself in the dictionnary
    // This is needed since the SharedMap within the Block
    // will need the block to be registered, so we have to
    // register it before it's fully created
    tblock = new(sharedMemoryBlock)
      T(key, (void*)((char*)sharedMemoryBlock + sizeof(T)));
  } else {
    tblock = (T*)sharedMemoryBlock;
    if(tblock != 0) {
      // Register block in the dictionnary. Here the block
      // was not created in this process. So we need to register
      // it in this process dictionnary.
      insertBlock(key, tblock, T::malloc, T::free);
    }
    // else the block was not created because create was false,
    // nothing to do
  }
  // Now we can release the lock
  Cmutex_unlock(&s_blockDict);
  // return the new Block, or 0 if nothing existed and create was false
  return tblock;
}

#endif // SHAREDMEMORY_BLOCKDICT_HPP
