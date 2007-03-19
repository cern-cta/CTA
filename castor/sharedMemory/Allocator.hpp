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
 * @(#)$RCSfile: Allocator.hpp,v $ $Revision: 1.15 $ $Release$ $Date: 2007/03/19 17:48:00 $ $Author: itglp $
 *
 * Allocator for the Shared Memory space
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_ALLOCATOR_HPP
#define SHAREDMEMORY_ALLOCATOR_HPP 1

#include <memory>
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Dlf.hpp"

namespace castor {

  namespace sharedMemory {

    // Forward declaration
    class LocalBlock;

    /**
     * A shared memory allocator to be used for allocating
     * space in shared memory in all STL containers.
     * @param T the type of objects to be allocated
     * @param BK a small class that should implement a static method
     * getBlockKey returning a BlockKey identifying the shared memory
     * Block to be used
     */
    template<typename T, typename BK>
    class Allocator : public std::allocator<T> {

    public:

      /**
       * default constructor
       */
      Allocator() throw() : std::allocator<T>(), m_smBlock(0) {};

      /**
       * templated constructor
       */
      template <class U> Allocator(const Allocator<U, BK>& a) throw() :
        std::allocator<T>(a), m_smBlock(a.m_smBlock) {}

      /**
       * destructor
       */
      ~Allocator() throw() {};

      /**
       * new operator, so that this goes always to shared memory
       */
      void *operator new(size_t num_bytes, void*)
	throw (castor::exception::Exception);

      /**
       * allocates objects in the shared memory
       * @param numObjects the number of objects to allocate
       * @param hint locality hint, ignored here
       * @return pointer to the allocated memory
       */
      T* allocate(std::allocator<void>::size_type numObjects,
                  std::allocator<void>::const_pointer hint = 0)
	throw(std::exception);

      /**
       * deallocates objects in the shared memory
       * @param ptrToMemory the memory address to deallocate
       * @param numObjects the number of objects to deallocate
       */
      void deallocate(std::allocator<void>::pointer ptrToMemory,
                      std::allocator<void>::size_type numObjects) throw();

      /**
       * some complicated way to define a needed typedef
       * See section 19.4.1 of Stroustrup
       */
      template <class U>
      struct rebind {typedef Allocator<U, BK> other; };

    private:

      /**
       * retrieves the shared Memory Block to be used
       * or create it and register it in the block
       * disctionnary if necessary
       */
      castor::sharedMemory::LocalBlock* getBlock()
	throw (castor::exception::Exception);

    // should be private but the templated constructor would not compile
    public:

      /// the shared memory block to be used by this allocator
      castor::sharedMemory::LocalBlock* m_smBlock;

    }; // class Allocator

  } // namespace sharedMemory

} // namespace castor

////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include <memory>
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/LocalBlock.hpp"
#include "castor/sharedMemory/BlockDict.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// operator new
//------------------------------------------------------------------------------
template<class T, class BK>
void* castor::sharedMemory::Allocator<T, BK>::operator new
(size_t num_bytes, void*)
  throw (castor::exception::Exception) {
  castor::sharedMemory::LocalBlock* smBlock = getBlock();
  return smBlock->malloc(num_bytes);
}

//------------------------------------------------------------------------------
// allocate
//------------------------------------------------------------------------------
template<class T, class BK>
T* castor::sharedMemory::Allocator<T, BK>::allocate
(std::allocator<void>::size_type numObjects,
 std::allocator<void>::const_pointer hint)
  throw(std::exception) {
  try {
    if (0 == m_smBlock) m_smBlock = getBlock();
    return static_cast<T*>
      (m_smBlock->malloc(numObjects*sizeof(T)));
  } catch (castor::exception::Exception e) {
    // Log exception "Exception caught in allocate"
    std::cout << e.getMessage().str() << std::endl;
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Original error", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
			    DLF_BASE_SHAREDMEMORY + 6, 1, initParams);
    // and throw standard exception
    throw std::bad_alloc();
  }
}


//------------------------------------------------------------------------------
// deallocate
//------------------------------------------------------------------------------
template<class T, class BK>
void castor::sharedMemory::Allocator<T, BK>::deallocate
(std::allocator<void>::pointer ptrToMemory,
 std::allocator<void>::size_type numObjects) throw() {
  try {
    if (0 == m_smBlock) m_smBlock = getBlock();
    m_smBlock->free(ptrToMemory, numObjects*sizeof(T));
  } catch (castor::exception::Exception e) {
    // Log exception "Exception caught in allocate"
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Original error", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG,
			    DLF_BASE_SHAREDMEMORY + 6, 1, initParams);
  }
}

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
template<class T, class BK>
castor::sharedMemory::LocalBlock*
castor::sharedMemory::Allocator<T, BK>::getBlock()
throw (castor::exception::Exception) {
  // try to get an existing block
  BlockKey key = BK::getBlockKey();
  castor::sharedMemory::LocalBlock* smBlock =
    castor::sharedMemory::BlockDict::getLocalBlock(key);
  if (0 == smBlock) {
    castor::exception::Internal e;
    e.getMessage() << "No block found for key " << key.key();
    throw e;
  }
  return smBlock;
}

#endif // SHAREDMEMORY_ALLOCATOR_HPP
