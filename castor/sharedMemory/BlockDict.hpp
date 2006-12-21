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
 * @(#)$RCSfile: BlockDict.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/12/21 15:37:48 $ $Author: sponcec3 $
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
    class IBlock;

    /**
     * Just a container for the dictionnary,
     */
    class BlockDict {

    public:

      /**
       * retrieves any a block by key. Does not create any new block
       * @return a pointer to the block or 0 if not found
       */
      static IBlock* getIBlock(BlockKey &key);

      /**
       * retrieves any kind of block by key
       * In case the block does not yet exist, creates it
       */
      template<typename T>
      static T* getBlock(BlockKey &key);

      /**
       * add a block
       */
      static bool insertBlock(BlockKey &key, IBlock* block);

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
      static std::map<BlockKey, IBlock*> s_blockDict;

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
  IBlock* block = getIBlock(key);
  if (0 != block) {
    T* tblock = dynamic_cast<T*>(block);
    if (0 == tblock) {
      // "Found block of bad type in BlockDict"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Key", key.key())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 0, 1, initParams);
      castor::exception::Internal e;
      e.getMessage() << "Found block of bad type in BlockDict.";
      throw e;
    }
    return tblock;
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
