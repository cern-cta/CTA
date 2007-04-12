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
 * @(#)$RCSfile: SingletonBlock.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2007/04/12 16:50:00 $ $Author: itglp $
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
     * @param T the type of the singleton object
     * @param A the allocator for the underlying block
     */
    template <typename T, typename A>
    class SingletonBlock : public Block<A> {

    public:

      /**
       * Constructor
       * Initiates a block of shared memory.
       * @param key the key for this block
       * @param rawMem pointer to the raw memory to be used
       * of the raw memory block
       */
      SingletonBlock(BlockKey& key, void* rawMem)
        throw (castor::exception::Exception);

      /**
       * Destructor
       */
      ~SingletonBlock() throw ();

      /**
       * accessor to the singleton object
       */
      T* getSingleton() { return m_singleton; }

      /**
       * Outputs this object in a human readable format
       * @param stream The stream where to print this object
       * @param indent The indentation to use
       */
      void print(std::ostream& stream,
		 std::string indent) const throw();

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
template<class T, class A>
castor::sharedMemory::SingletonBlock<T,A>::SingletonBlock
(castor::sharedMemory::BlockKey& key, void* rawMem)
  throw (castor::exception::Exception) :
  Block<A>(key, (void*)((char*)rawMem + sizeof(T))) {
  m_singleton = new(rawMem)T();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
template<class T, class A>
castor::sharedMemory::SingletonBlock<T,A>::~SingletonBlock () throw () {}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
template<class T, class A>
void castor::sharedMemory::SingletonBlock<T,A>::print
(std::ostream& stream, std::string indent) const throw() {
  castor::sharedMemory::Block<A>::print(stream, indent);
  stream << indent << "[# SingletonBlock #]" << "\n"
	 << indent << "  m_singleton = ";
  m_singleton->print(stream, indent + "  ");
}

#endif // SHAREDMEMORY_SINGLETONBLOCK_HPP
