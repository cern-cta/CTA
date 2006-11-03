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
 * @(#)$RCSfile: BlockDict.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/11/03 15:34:38 $ $Author: sponcec3 $
 *
 * A static dictionnary of blocks, referenced by their
 * BlockKey
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <map>
#include "Cthread_api.h"
#include "castor/sharedMemory/BlockKey.hpp"
#include "castor/sharedMemory/BlockDict.hpp"

//------------------------------------------------------------------------------
// s_blockDict
//------------------------------------------------------------------------------
std::map<castor::sharedMemory::BlockKey, castor::sharedMemory::IBlock*>
castor::sharedMemory::BlockDict::s_blockDict;

//------------------------------------------------------------------------------
// getBlock
//------------------------------------------------------------------------------
castor::sharedMemory::IBlock*
castor::sharedMemory::BlockDict::getBlock
(castor::sharedMemory::BlockKey &key) {
  std::map<BlockKey, IBlock*>::iterator it =
    s_blockDict.find(key);
  if (it == s_blockDict.end()) return 0;
  return it->second;
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

//------------------------------------------------------------------------------
// lock
//------------------------------------------------------------------------------
void castor::sharedMemory::BlockDict::lock() {
  Cthread_mutex_lock(&s_blockDict);
}

//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void castor::sharedMemory::BlockDict::unlock() {
  Cthread_mutex_unlock(&s_blockDict);
}
