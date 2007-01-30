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
 * @(#)$RCSfile: BlockDict.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/01/30 09:25:35 $ $Author: sponcec3 $
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
    class BasicBlock;

    /**
     * Just a container for the dictionnary,
     */
    class BlockDict {

    public:

      /**
       * retrieves any a block by key. Does not create any new block
       * @return a pointer to the block or 0 if not found
       */
      static BasicBlock* getBasicBlock(BlockKey &key);

      /**
       * retrieves any kind of block by key
       * In case the block does not yet exist, creates it
       * This method is to be used with care since it does
       * a static cast of the block found for key into the
       * desired type, without any further check
       */
      template<typename T>
      static T* getBlock(BlockKey &key);

      /**
       * add a block
       */
      static bool insertBlock(BlockKey &key, BasicBlock* block);

    private:

      /**
       * create a block
       * @param pointer is filled with a pointer to the raw memory of the block
       * @return whether the block was really created or only attached
       * (true means really created)
       */
      static bool createBlock(BlockKey &key, void** pointer);

    private:

      /**
       * the dictionnary of existing blocks
       */
      static std::map<BlockKey, BasicBlock*> s_blockDict;

    }; // class BlockDict

  } // namespace sharedMemory

} // namespace castor

////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include "castor/exception/Internal.hpp"
#include "castor/dlf/Dlf.hpp"
#include <Cmutex.h>

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
template <typename T>
T* castor::sharedMemory::BlockDict::getBlock
(castor::sharedMemory::BlockKey &key) {
  BasicBlock* block = getBasicBlock(key);
  if (0 != block) {
    return (T*)block;
  }
  // here we will have to create a new block and register it
  // we need to assure that we create the shared memory only once
  // so we serialize all the Block creation
  Cmutex_lock(&s_blockDict, -1);
  // create the block
  void* sharedMemoryBlock;
  bool created = createBlock(key, &sharedMemoryBlock);
  // Now let's really create/retrieve the Block object inside the shared memory
  T* tblock = 0;
  if (created) {
    // Object creation
    // Note that it will register itself in the dictionnary
    tblock = new(sharedMemoryBlock)
      T(key, (void*)((char*)sharedMemoryBlock + sizeof(T)));
  } else {
    tblock = (T*)sharedMemoryBlock;
  }
  // Now we can release the lock
  Cmutex_unlock(&s_blockDict);
  // return the brean new Block
  return tblock;
}

#endif // SHAREDMEMORY_BLOCKDICT_HPP
