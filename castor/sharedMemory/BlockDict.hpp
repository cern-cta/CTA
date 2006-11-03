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
 * @(#)$RCSfile: BlockDict.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/11/03 15:34:38 $ $Author: sponcec3 $
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
    class IAllocator;

    /**
     * Just a container for the dictionnary,
     * with locking facility for thread safe access
     */
    class BlockDict {

    public:

      /**
       * retrieves a block by key
       */
      static IBlock* getBlock(BlockKey &key);

      /**
       * add a block
       */
      static bool insertBlock(BlockKey &key, IBlock* block);

      /**
       * lock the dictionnary
       */
      static void lock();

      /**
       * unlock the dictionnary
       */
      static void unlock();

    private:

      /**
       * the dictionnary of existing blocks
       */
      static std::map<BlockKey, IBlock*> s_blockDict;

    }; // class BlockDict

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_BLOCKDICT_HPP
