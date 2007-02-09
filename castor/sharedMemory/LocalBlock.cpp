/******************************************************************************
 *                      LocalBlock.cpp
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
 * @(#)$RCSfile: LocalBlock.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/02/09 16:59:19 $ $Author: sponcec3 $
 *
 * An object, usually created in process memory (not in
 * a shared memory block) encapsulating a sharedMemory
 * Block
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/sharedMemory/LocalBlock.hpp"
#include "castor/sharedMemory/BasicBlock.hpp"
#include "castor/exception/Internal.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::sharedMemory::LocalBlock::LocalBlock
(castor::sharedMemory::BasicBlock* basicBlock,
 void* (*mallocMethod)(castor::sharedMemory::BasicBlock* obj, size_t nbBytes),
 void (*freeMethod)(castor::sharedMemory::BasicBlock* obj, void* pointer, size_t nbBytes))
  throw (castor::exception::Exception) :
  m_block(basicBlock), m_mallocMethod(mallocMethod),
  m_freeMethod(freeMethod) {}

//------------------------------------------------------------------------------
// malloc
//------------------------------------------------------------------------------
void* castor::sharedMemory::LocalBlock::malloc(size_t nbBytes)
  throw (castor::exception::Exception) {
  if (0 == m_mallocMethod) {
    castor::exception::Internal e;
    e.getMessage() << "Tried to malloc bytes in an abstract block";
    throw e;
  }
  return m_mallocMethod(m_block, nbBytes);
}

//------------------------------------------------------------------------------
// free
//------------------------------------------------------------------------------
void castor::sharedMemory::LocalBlock::free
(void* pointer, size_t nbBytes)
  throw (castor::exception::Exception) {
  if (0 == m_freeMethod) {
    castor::exception::Internal e;
    e.getMessage() << "Tried to free bytes in an abstract block";
    throw e;
  }
  return m_freeMethod(m_block, pointer, nbBytes);
}
