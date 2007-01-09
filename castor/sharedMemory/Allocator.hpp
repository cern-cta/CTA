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
 * @(#)$RCSfile: Allocator.hpp,v $ $Revision: 1.10 $ $Release$ $Date: 2007/01/09 16:21:05 $ $Author: sponcec3 $
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
        std::allocator<T>(a), m_smBlock(a.m_smBlock) {}

      /**
       * destructor
       */
      virtual ~Allocator() throw() {};

      /**
       * new operator, so that this goes always to shared memory
       */
      void *operator new(unsigned int num_bytes, void*);

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
       * returns the key for the shared memory block to be used
       */
      virtual BlockKey getBlockKey() = 0;

    private:

      /**
       * retrieves the shared Memory Block to be used
       * or create it and register it in the block
       * disctionnary if necessary
       */
      castor::sharedMemory::IBlock* getBlock();

    // should be private but the templated constructor would not compile
    public:

      /// the shared memory block to be used by this allocator
      castor::sharedMemory::IBlock* m_smBlock;

    }; // class Allocator

  } // namespace sharedMemory

} // namespace castor

////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include <memory>
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/IBlock.hpp"
#include "castor/sharedMemory/BlockDict.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// operator new
//------------------------------------------------------------------------------
template<class T>
void* castor::sharedMemory::Allocator<T>::operator new
(unsigned int num_bytes, void*) {
  castor::sharedMemory::IBlock* smBlock = getBlock();
  return smBlock->malloc(num_bytes);
}

//------------------------------------------------------------------------------
// allocate
//------------------------------------------------------------------------------
template<class T>
T* castor::sharedMemory::Allocator<T>::allocate
(std::allocator<void>::size_type numObjects,
 std::allocator<void>::const_pointer hint) {
  if (0 == m_smBlock) m_smBlock = getBlock();
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
  if (0 == m_smBlock) m_smBlock = getBlock();
  m_smBlock->free(ptrToMemory, numObjects*sizeof(T));
}

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::IBlock*
castor::sharedMemory::Allocator<T>::getBlock() {
  // try to get an existing block
  BlockKey key = getBlockKey();
  castor::sharedMemory::IBlock* smBlock =
    castor::sharedMemory::BlockDict::getIBlock(key);
  if (0 == smBlock) {
    castor::exception::Internal e;
    e.getMessage() << "No block found for key " << key.key();
    throw e;
  }
  return smBlock;
}

#endif // SHAREDMEMORY_ALLOCATOR_HPP
