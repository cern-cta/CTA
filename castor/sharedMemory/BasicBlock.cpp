/******************************************************************************
 *                      BasicBlock.cpp
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
 * @(#)$RCSfile: BasicBlock.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/01/30 09:25:35 $ $Author: sponcec3 $
 *
 * A basic shared memory block, with a key and a static
 * table of attached addresses
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/sharedMemory/BasicBlock.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::sharedMemory::BasicBlock::BasicBlock
(BlockKey& key, void* rawMem)
  throw (castor::exception::Exception) :
  m_key(key), m_sharedMemoryBlock(rawMem),
m_mallocMethod(0), m_freeMethod(0) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::sharedMemory::BasicBlock::~BasicBlock () throw () {}

//------------------------------------------------------------------------------
// attached blocks static member
//------------------------------------------------------------------------------
std::map<int, void*> castor::sharedMemory::BasicBlock::s_attachedBlocks;

//------------------------------------------------------------------------------
// malloc
//------------------------------------------------------------------------------
void* castor::sharedMemory::BasicBlock::malloc(size_t nbBytes)
  throw (castor::exception::Exception) {
  if (0 == m_mallocMethod) {
    castor::exception::Internal e;
    e.getMessage() << "Tried to malloc bytes in an abstract block";
    throw e;
  }
  return m_mallocMethod(this, nbBytes);
}

//------------------------------------------------------------------------------
// free
//------------------------------------------------------------------------------
void castor::sharedMemory::BasicBlock::free
(void* pointer, size_t nbBytes)
  throw (castor::exception::Exception) {
  if (0 == m_freeMethod) {
    castor::exception::Internal e;
    e.getMessage() << "Tried to free bytes in an abstract block";
    throw e;
  }
  return m_freeMethod(this, pointer, nbBytes);
}
