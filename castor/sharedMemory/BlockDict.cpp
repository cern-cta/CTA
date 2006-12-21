/******************************************************************************
 *                      BlockDict.cpp
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
 * @(#)$RCSfile: BlockDict.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2006/12/21 15:37:48 $ $Author: sponcec3 $
 *
 * A static dictionnary of blocks, referenced by their
 * BlockKey
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <map>
#include "castor/dlf/Dlf.hpp"
#include "castor/sharedMemory/IBlock.hpp"
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/sharedMemory/BlockDict.hpp"
#include "castor/exception/Internal.hpp"
#include "Cthread_api.h"
#include <sys/ipc.h>
#include <sys/shm.h>
#include <errno.h>

//------------------------------------------------------------------------------
// s_blockDict
//------------------------------------------------------------------------------
std::map<castor::sharedMemory::BlockKey, castor::sharedMemory::IBlock*>
castor::sharedMemory::BlockDict::s_blockDict;

//------------------------------------------------------------------------------
// getIBlock
//------------------------------------------------------------------------------
castor::sharedMemory::IBlock*
castor::sharedMemory::BlockDict::getIBlock
(castor::sharedMemory::BlockKey &key) {
  // try to find the block in existing ones
  std::map<BlockKey, IBlock*>::const_iterator it =
    s_blockDict.find(key);
  if (it != s_blockDict.end()) {
    return it->second;
  }
  return 0;
}

//------------------------------------------------------------------------------
// createBlock
//------------------------------------------------------------------------------
bool castor::sharedMemory::BlockDict::createBlock
(castor::sharedMemory::BlockKey &key, void** pointer) {
  bool created = false;
  // Try to get the identifier of the shared memory
  int shmid = shmget(key.key(), key.size(), 0666);
  if (-1 == shmid) {
    if (ENOENT != errno) {
      // "Unable to get shared memory id. Giving up"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Error Message", strerror(errno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 0, 1, initParams);
      castor::exception::Internal e;
      e.getMessage() << "Unable to get shared memory id. Giving up.";
      throw e;
    }
    // Try to create the shared memory
    shmid = shmget(key.key(), key.size(), IPC_CREAT | 0666);
    if (-1 == shmid) {
      // "Unable to create shared memory. Giving up"
      castor::dlf::Param initParams[] =
	{castor::dlf::Param("Error Message", strerror(errno))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1, 1, initParams);
      castor::exception::Internal e;
      e.getMessage() << "Unable to create shared memory. Giving up.";
      throw e;
    }
    // "Created the shared memory."
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 0, 0);
    created = true;
  }
  // Attach the shared memory
  void *sharedMemoryBlock = shmat(shmid, key.address(), SHM_REMAP);
  if (-1 == (int)sharedMemoryBlock) {
    // "Unable to get pointer to shared memory. Giving up"
    castor::dlf::Param initParams[] =
      {castor::dlf::Param("Error Message", strerror(errno))};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 1, initParams);
    castor::exception::Internal e;
    e.getMessage() << "Unable to get pointer to shared memory. Giving up.";
    throw e;
  }
  *pointer = sharedMemoryBlock;
  return created;
}

//------------------------------------------------------------------------------
// insertBlock
//------------------------------------------------------------------------------
bool
castor::sharedMemory::BlockDict::insertBlock
(castor::sharedMemory::BlockKey &key,
 castor::sharedMemory::IBlock *block) {
  return s_blockDict.insert
    (std::pair<BlockKey, IBlock*>(key, block)).second;
}
