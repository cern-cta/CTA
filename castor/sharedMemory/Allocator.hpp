/******************************************************************************
 *                      Allocator.hpp
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
 * @(#)$RCSfile: Allocator.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/10/26 13:33:30 $ $Author: felixehm $
 *
 * Allocator for the Shared Memory space
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_ALLOCATOR_HPP
#define SHAREDMEMORY_ALLOCATOR_HPP 1

#include <memory>
#include "castor/sharedMemory/BlockKey.hpp"

namespace castor {

  namespace sharedMemory {

    // Forward declaration
    class Block;

    /**
     * A shared memory allocator to be used for allocating
     * space in shared memory in all STL containers.
     * Note that this is an abstract class and that the
     * non abstract children have to implement the getKey
     * method. This will bind them to a given shared memory
     * block while this class is generic.
     * The reason for this is that the STL creates allocator
     * with the empty constructor and imposes thus to store
     * the block key in the type of the allocator itself.
     */
    template<typename T>
    class Allocator : public std::allocator<T> {

    public:

      /**
       * default constructor
       */
      Allocator() throw() : std::allocator<T>(), m_smBlock(0) {};

      /**
       * templated constructor
       */
      template <class U> Allocator(const Allocator<U>& a) throw() :
        std::allocator<T>(a) {};

      /**
       * destructor
       */
      virtual ~Allocator() throw();

      /**
       * allocates objects in the shared memory
       * @param numObjects the number of objects to allocate
       * @param hint locality hint, ignored here
       * @return pointer to the allocated memory
       */
      T* allocate(std::allocator<void>::size_type numObjects,
                  std::allocator<void>::const_pointer hint = 0);

      /**
       * deallocates objects in the shared memory
       * @param ptrToMemory the memory address to deallocate
       * @param numObjects the number of objects to deallocate
       */
      void deallocate(std::allocator<void>::pointer ptrToMemory,
                      std::allocator<void>::size_type numObjects);

      /**
       * returns identification of the shared memory block
       * to be used by this allocator
       */
      virtual BlockKey getBlockKey() = 0;

    private:

      /**
       * created the internal shared memory Block
       */
      inline Block* createSharedMemoryBlock();

    private:

      /// the shared memory block to be used by this allocator
      castor::sharedMemory::Block* m_smBlock;

    }; // class Allocator

  } // namespace sharedMemory

} // namespace castor


////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include <memory>
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/Block.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::Allocator<T>::~Allocator() throw() {
  if (0 != m_smBlock) {
    delete m_smBlock;
  }
}

//------------------------------------------------------------------------------
// createSharedMemoryBlock
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::Block*
castor::sharedMemory::Allocator<T>::createSharedMemoryBlock() {
  castor::sharedMemory::BlockKey key = getBlockKey();
  m_smBlock = new Block(key.key(),
                        key.size(),
                        key.address());
}

//------------------------------------------------------------------------------
// allocate
//------------------------------------------------------------------------------
template<class T>
T* castor::sharedMemory::Allocator<T>::allocate
(std::allocator<void>::size_type numObjects,
 std::allocator<void>::const_pointer hint) {
  if (0 == m_smBlock) {
    // we should create the block
    m_smBlock = createSharedMemoryBlock();
  }
  return static_cast<T*>
    (m_smBlock->malloc(numObjects*sizeof(T)));
}


//------------------------------------------------------------------------------
// deallocate
//------------------------------------------------------------------------------
template<class T>
void castor::sharedMemory::Allocator<T>::deallocate
(std::allocator<void>::pointer ptrToMemory,
 std::allocator<void>::size_type numObjects) {
  if (0 == m_smBlock) {
    // we should create the block
    m_smBlock = createSharedMemoryBlock();
  }
  m_smBlock->free(ptrToMemory, numObjects*sizeof(T));
}

#endif // SHAREDMEMORY_ALLOCATOR_HPP
