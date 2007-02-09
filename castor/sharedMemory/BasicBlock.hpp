/******************************************************************************
 *                      BasicBlock.hpp
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
 * @(#)$RCSfile: BasicBlock.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2007/02/09 16:59:19 $ $Author: sponcec3 $
 *
 * A basic shared memory block, with a key and a static
 * table of attached addresses
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_BASICBLOCK_HPP
#define SHAREDMEMORY_BASICBLOCK_HPP 1

#include <map>
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    /**
     * a class dealing with a basic Block of Shared memory.
     * Any real Block implementation should extend this class
     * by defining a free and a malloc function. (see example
     * and signature in Block.hpp)
     * These are not defined here as abstract to avoid having
     * a virtual table, which is kind of forbiden when using
     * shared memory.
     * In order to replace the virtual table, the BlockDict
     * object registers, for each block the actual free and
     * malloc functions, and acts as a local virtual table
     */
    class BasicBlock {

    public:

      /**
       * Constructor
       * @param key the key for this block
       * @param rawMem pointer to the raw memory to be used
       * header of the Block.
       */
      BasicBlock(BlockKey& key, void* rawMem)
        throw (castor::exception::Exception);

      /**
       * Destructor
       */
      ~BasicBlock() throw();

    protected:

      /**
       * key of the block. This includes the actual key,
       * the size of the block and the desired mapping address
       * (may be null)
       */
      BlockKey m_key;

      /**
       * List of already attached memory blocks
       */
      static std::map<int, void*> s_attachedBlocks;

      /**
       * Pointer to the raw shared memory block
       */
      void* m_sharedMemoryBlock;

    }; // end class BasicBlock

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_BASICBLOCK_HPP
