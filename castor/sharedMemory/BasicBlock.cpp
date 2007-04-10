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
 * @(#)$RCSfile: BasicBlock.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2007/04/10 16:23:47 $ $Author: sponcec3 $
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
  m_key(key), m_sharedMemoryBlock(rawMem) {}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::sharedMemory::BasicBlock::~BasicBlock () throw () {}

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::sharedMemory::BasicBlock::print(std::ostream& stream,
					     std::string indent) const
  throw() {
  stream << indent << "[# BasicBlock #]" << "\n";
  m_key.print(stream, indent + "  ");
  stream << indent << "  m_sharedMemoryBlock = "
	 << std::hex << m_sharedMemoryBlock
	 << std::endl;
}

//------------------------------------------------------------------------------
// attached blocks static member
//------------------------------------------------------------------------------
std::map<int, void*> castor::sharedMemory::BasicBlock::s_attachedBlocks;


