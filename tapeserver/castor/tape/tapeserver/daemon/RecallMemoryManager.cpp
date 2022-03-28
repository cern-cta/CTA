/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"


namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RecallMemoryManager::RecallMemoryManager(const size_t numberOfBlocks, const size_t blockSize, cta::log::LogContext&  lc)
: m_totalNumberOfBlocks(numberOfBlocks), m_lc(lc) {
  for (size_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, blockSize));

    //m_lc.pushOrReplace(cta::log::Param("blockId", i));
    //m_lc.log(cta::log::DEBUG, "RecallMemoryManager created a block");
  }
  cta::log::ScopedParamContainer params(m_lc);
  params.add("blockCount", numberOfBlocks)
        .add("blockSize", blockSize)
        .add("totalSize", numberOfBlocks*blockSize);
  m_lc.log(cta::log::INFO, "RecallMemoryManager: all blocks have been created");
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

  m_lc.log(cta::log::INFO, "RecallMemoryManager destruction : all memory blocks have been deleted");
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
  //m_lc.pushOrReplace(cta::log::Param("blockId", mb->m_memoryBlockId));
  //m_lc.log(cta::log::DEBUG, "RecallMemoryManager A block has been released");
  mb->reset();
  m_freeBlocks.push(mb);
}

}
}
}
}
