/******************************************************************************
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RecallMemoryManager::RecallMemoryManager(const size_t numberOfBlocks, const size_t blockSize, castor::log::LogContext& lc)
: m_totalNumberOfBlocks(numberOfBlocks), m_lc(lc) {
  for (size_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, blockSize));

    //m_lc.pushOrReplace(log::Param("blockId", i));
    //m_lc.log(LOG_DEBUG, "RecallMemoryManager created a block");
  }
  log::ScopedParamContainer params(m_lc);
  params.add("blockCount", numberOfBlocks)
        .add("blockSize", blockSize)
        .add("totalSize", numberOfBlocks*blockSize);
  m_lc.log(LOG_INFO, "RecallMemoryManager: all blocks have been created");
}

//------------------------------------------------------------------------------
// RecallMemoryManager::~RecallMemoryManager
//------------------------------------------------------------------------------
RecallMemoryManager::~RecallMemoryManager() {
  // Make sure the thread is finished: this should be done by the caller,
  // who should have called waitThreads.
  // castor::server::Thread::wait();
  // we expect to be called after all users are finished. Just "free"
  // the memory blocks we still have.

  cta::threading::BlockingQueue<MemBlock*>::valueRemainingPair ret;
  do {
    ret = m_freeBlocks.popGetSize();
    delete ret.value;
  } while (ret.remaining > 0);

  m_lc.log(LOG_INFO, "RecallMemoryManager destruction : all memory blocks have been deleted");
}

//------------------------------------------------------------------------------
// RecallMemoryManager::~RecallMemoryManager
//------------------------------------------------------------------------------
bool RecallMemoryManager::areBlocksAllBack() throw() {
  return m_totalNumberOfBlocks == m_freeBlocks.size();
}

//------------------------------------------------------------------------------
// RecallMemoryManager::~RecallMemoryManager
//------------------------------------------------------------------------------
MemBlock* RecallMemoryManager::getFreeBlock() {
  MemBlock* ret = m_freeBlocks.pop();
  // When delivering a fresh block to the user, it should be empty.
  if (ret->m_payload.size()) {
    m_freeBlocks.push(ret);
    throw cta::exception::Exception(
      "Internal error: RecallMemoryManager::getFreeBlock "
      "popped a non-empty memory block");
  }
  return ret;
}

//------------------------------------------------------------------------------
// RecallMemoryManager::~RecallMemoryManager
//------------------------------------------------------------------------------
void RecallMemoryManager::releaseBlock(MemBlock* mb) {
  //m_lc.pushOrReplace(log::Param("blockId", mb->m_memoryBlockId));
  //m_lc.log(LOG_DEBUG, "RecallMemoryManager A block has been released");
  mb->reset();
  m_freeBlocks.push(mb);
}

}
}
}
}
