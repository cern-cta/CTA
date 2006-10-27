/******************************************************************************
 *                      SingletonBlock.hpp
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
 * @(#)$RCSfile: SingletonBlock.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/10/27 15:28:03 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef SHAREDMEMORY_SINGLETONBLOCK_HPP 
#define SHAREDMEMORY_SINGLETONBLOCK_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/sharedMemory/Block.hpp"

namespace castor {

  namespace sharedMemory {

    /**
     * a class dealing with a Block of Shared memory including
     * a singleton object of type T. This is actually a kind
     * of super singleton where the object is shared by all
     * threads of all processes using this block.
     */
    template <typename T>
    class SingletonBlock : public Block {

    public:

      /**
       * Constructor
       * Initiates a block of shared memory.
       * @param key the key for this block
       */
      SingletonBlock(BlockKey& key)
        throw (castor::exception::Exception);

      /**
       * accessor to tyhe singleton object
       */
      T* getSingleton() { return m_singleton; }

    private:

      /**
       * the singleton object
       */
      T* m_singleton;

    }; // end class SingletonBlock

  } // namespace sharedMemory

} // namespace castor


////////////////////////////////////////////////////////////////////////////////
// Implementation of templated parts
////////////////////////////////////////////////////////////////////////////////

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
template<class T>
castor::sharedMemory::SingletonBlock<T>::SingletonBlock
(castor::sharedMemory::BlockKey& key)
  throw (castor::exception::Exception) :
  Block(key) {
  // Create the singleton object
  m_singleton =
    static_cast<T*>(this->malloc(sizeof(T)));
};

#endif // SHAREDMEMORY_SINGLETONBLOCK_HPP
