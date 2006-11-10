/******************************************************************************
 *                      SharedMemoryAllocator.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * A shared memory allocator dedicated to monitoring
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef MONITORING_SHAREDMEMORYALLOCATOR_HPP 
#define MONITORING_SHAREDMEMORYALLOCATOR_HPP 1

#include "castor/sharedMemory/IBlock.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"

namespace castor {

  namespace monitoring {

    /**
     * An allocator for the shaerd memory block describing
     * the cluster status
     */
    template<typename T>
    class SharedMemoryAllocator :
      public castor::sharedMemory::Allocator<T> {

    public:

      /**
       * default constructor
       */
      SharedMemoryAllocator()
        throw() : castor::sharedMemory::Allocator<T>() {};

      /**
       * templated constructor
       */
      template <class U> SharedMemoryAllocator
      (const SharedMemoryAllocator<U>& a) throw() :
        castor::sharedMemory::Allocator<T>(a) {};

      /**
       * some complicated way to define a needed typedef
       * See section 19.4.1 of Stroustrup
       */
      template <class U>
      struct rebind {typedef SharedMemoryAllocator<U> other; };

    protected:

      /**
       * creates the internal shared memory Block
       */
      virtual castor::sharedMemory::IBlock* createSharedMemoryBlock();

      /**
       * returns the key for the shared memory block to be used
       */
      virtual castor::sharedMemory::BlockKey getBlockKey();

    }; // class SharedMemoryAllocator

  } // namespace monitoring

} // namespace castor


////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

#include "castor/sharedMemory/Block.hpp"

//------------------------------------------------------------------------------
// createSharedMemoryBlock
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::IBlock*
castor::monitoring::SharedMemoryAllocator<T>::createSharedMemoryBlock() {
  castor::sharedMemory::BlockKey key = getBlockKey();
  return new castor::sharedMemory::Block
    <SharedMemoryAllocator<castor::sharedMemory::SharedNode> >(key);
}

//------------------------------------------------------------------------------
// getBlockKey
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::BlockKey
castor::monitoring::SharedMemoryAllocator<T>::getBlockKey() {
  return castor::monitoring::getClusterStatusBlockKey();
}

#endif // MONITORING_SHAREDMEMORYALLOCATOR_HPP
