/******************************************************************************
 *                      Helper.cpp
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
 * @(#)$RCSfile: Helper.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/10/09 15:41:22 $ $Author: sponcec3 $
 *
 * A singleton for the shared memory usage in rmMaster
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/sharedMemory/Helper.hpp"
#include "castor/sharedMemory/Block.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/dlf/Dlf.hpp"
#include "errno.h"
#include <sys/ipc.h>
#include <sys/shm.h>

#define SHARED_MEMORY_SIZE 1048576
#define SHARED_MEMORY_KEY 2374
#define SHARED_MEMORY_ADDRESS 0x30000000

//------------------------------------------------------------------------------
// s_smBlockAddress
//------------------------------------------------------------------------------
void* castor::sharedMemory::Helper::s_smBlockAddress = 0;

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
castor::sharedMemory::Block* castor::sharedMemory::Helper::getBlock()
  throw (castor::exception::Exception) {
  // Check whether the block needs to be created
  if (0 == s_smBlockAddress) {
    Block* b =
      new Block(SHARED_MEMORY_KEY, SHARED_MEMORY_SIZE, (void*)SHARED_MEMORY_ADDRESS);
    s_smBlockAddress = b;
    b->initialize();
  }
  return (castor::sharedMemory::Block*)s_smBlockAddress;
}
