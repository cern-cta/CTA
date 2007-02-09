/******************************************************************************
 *                      LocalBlock.hpp
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
 * @(#)$RCSfile: LocalBlock.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/02/09 16:59:19 $ $Author: sponcec3 $
 *
 * An object, usually created in process memory (not in
 * a shared memory block) encapsulating a sharedMemory
 * Block
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_LOCALBLOCK_HPP
#define SHAREDMEMORY_LOCALBLOCK_HPP 1

#include "castor/exception/Exception.hpp"

namespace castor {

  namespace sharedMemory {

    // Forward declaration
    class BasicBlock;

    /**
     * An object, usually created in process memory (not in
     * a shared memory block) encapsulating a sharedMemory
     * Block. It basically holds a pointer to the block,
     * on to its malloc and one to its free method and
     * bahaves like the blco itself.
     * This is a way to get around virtual tables for the
     * Block cause we cannot really store them in shared memory
     */
    class LocalBlock {

    public:

      /**
       * Constructor
       * @param key the key for this block
       * @param rawMem pointer to the raw memory to be used
       * header of the Block.
       */
      LocalBlock
      (BasicBlock* basicBlock,
       void* (*mallocMethod)(BasicBlock* obj, size_t nbBytes),
       void (*freeMethod)(BasicBlock* obj, void* pointer, size_t nbBytes))
        throw (castor::exception::Exception);

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
       * accessor to block
       */
      BasicBlock* block() { return m_block; }

      /**
       * accessor to mallocMethod
       */
      void* (*mallocMethod())(BasicBlock* obj, size_t nbBytes)
      { return m_mallocMethod; }

      /**
       * accessor to freeMethod
       */
      void (*freeMethod())(BasicBlock* obj, void* pointer, size_t nbBytes)
      { return m_freeMethod; }

    private:

      /**
       * The underlying block
       */
      BasicBlock* m_block;

      /**
       * Pointer to the actual malloc method to be used
       */
      void* (*m_mallocMethod)(BasicBlock* obj, size_t nbBytes)
	throw (castor::exception::Exception);

      /**
       * Pointer to the actual free method to be used
       */
      void (*m_freeMethod)(BasicBlock* obj, void* pointer, size_t nbBytes)
	throw (castor::exception::Exception);

    }; // end class LocalBlock

  } // namespace sharedMemory

} // namespace castor

#endif // SHAREDMEMORY_LOCALBLOCK_HPP
