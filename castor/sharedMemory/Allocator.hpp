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
 * @(#)$RCSfile: Allocator.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/09/25 09:21:22 $ $Author: sponcec3 $
 *
 * Allocator for the Shared Memory space
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_ALLOCATOR_HPP
#define SHAREDMEMORY_ALLOCATOR_HPP 1

#include <memory>

namespace castor {

  namespace sharedMemory {

    /**
     * A shared memory allocator to be used for allocating
     * space in shared memory in all STL containers
     */
    template<typename T>
    class Allocator : public std::allocator<T> {

    public:
      
      /**
       * default constructor
       */
      Allocator() throw() : std::allocator<T>() {};

      /**
       * templated constructor
       */
      template <class U> Allocator(const Allocator<U>& a) throw() :
        std::allocator<T>(a) {};

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
       * some complicated way to define a needed typedef
       * See section 19.4.1 of Stroustrup
       */
      template <class U>
      struct rebind {typedef Allocator<U> other; };

    }; // class Allocator

  } // namespace sharedMemory

} // namespace castor


////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/Helper.hpp"
#include "castor/sharedMemory/Block.hpp"

//------------------------------------------------------------------------------
// allocate
//------------------------------------------------------------------------------
template<class T>
T* castor::sharedMemory::Allocator<T>::allocate
(std::allocator<void>::size_type numObjects,
 std::allocator<void>::const_pointer hint) {
  return static_cast<T*>
    (castor::sharedMemory::Helper::getBlock()->
     malloc(numObjects*sizeof(T)));
}


//------------------------------------------------------------------------------
// deallocate
//------------------------------------------------------------------------------
template<class T>
void castor::sharedMemory::Allocator<T>::deallocate
(std::allocator<void>::pointer ptrToMemory,
 std::allocator<void>::size_type numObjects) {
  castor::sharedMemory::Helper::getBlock()->free
    (ptrToMemory, numObjects*sizeof(T));
}

#endif // SHAREDMEMORY_ALLOCATOR_HPP
