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

#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
MigrationMemoryManager::MigrationMemoryManager(const uint32_t numberOfBlocks, const uint32_t blockSize,
                                               const cta::log::LogContext& lc) :
m_blockCapacity(blockSize),
m_totalNumberOfBlocks(0), m_totalMemoryAllocated(0), m_blocksProvided(0), m_blocksReturned(0), m_lc(lc) {

  for (uint32_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, blockSize));
    m_totalNumberOfBlocks++;
    m_totalMemoryAllocated += blockSize;
  }
  m_lc.log(cta::log::INFO, "MigrationMemoryManager: all blocks have been created");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::~MigrationMemoryManager
//------------------------------------------------------------------------------
MigrationMemoryManager::~MigrationMemoryManager() noexcept {
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

  m_lc.log(cta::log::INFO, "MigrationMemoryManager destruction : all memory blocks have been deleted");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::startThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::startThreads()  {
  cta::threading::Thread::start();
  m_lc.log(cta::log::INFO, "MigrationMemoryManager starting thread");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::waitThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::waitThreads()  {
  cta::threading::Thread::wait();
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::addClient
//------------------------------------------------------------------------------
void MigrationMemoryManager::addClient(DataPipeline* c)
 {
  m_clientQueue.push(c);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::areBlocksAllBack
//------------------------------------------------------------------------------
bool MigrationMemoryManager::areBlocksAllBack() noexcept{
  return m_totalNumberOfBlocks == m_freeBlocks.size();
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::blockCapacity
//------------------------------------------------------------------------------
size_t MigrationMemoryManager::blockCapacity() {
  return m_blockCapacity;
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::finish
//------------------------------------------------------------------------------
void MigrationMemoryManager::finish()
 {
  addClient(nullptr);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::releaseBlock
//------------------------------------------------------------------------------
void MigrationMemoryManager::releaseBlock(MemBlock* mb)
 {
  mb->reset();
  m_freeBlocks.push(mb);
  {
    cta::threading::MutexLocker ml(m_countersMutex);
    m_blocksReturned++;
  }
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::run
//------------------------------------------------------------------------------
void MigrationMemoryManager::run()  {
  while (true) {
    DataPipeline* c = m_clientQueue.pop();
    // If the c is a nullptr pointer, that means end of clients
    if (!c) return;
    // Spin on the the client. We rely on the fact that he will want
    // at least one block (which is the case currently)
    while (c->provideBlock(m_freeBlocks.pop())) {
      cta::threading::MutexLocker ml(m_countersMutex);
      m_blocksProvided++;
    }
  }
}

}
}
}
}
