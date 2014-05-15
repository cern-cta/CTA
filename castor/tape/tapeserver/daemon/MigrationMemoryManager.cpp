/******************************************************************************
 *                      MigrationMemoryManager.cpp
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
MigrationMemoryManager::MigrationMemoryManager(const size_t numberOfBlocks, 
    const size_t blockSize, castor::log::LogContext lc)
:
    m_blockCapacity(blockSize), m_totalNumberOfBlocks(0),
    m_totalMemoryAllocated(0), m_blocksProvided(0), 
    m_blocksReturned(0), m_lc(lc)
{
  for (size_t i = 0; i < numberOfBlocks; i++) {
    m_freeBlocks.push(new MemBlock(i, blockSize));
    m_totalNumberOfBlocks++;
    m_totalMemoryAllocated += blockSize;

    m_lc.pushOrReplace(log::Param("blockId", i));
    m_lc.log(LOG_DEBUG, "MigrationMemoryManager Created a block");
  }
  m_lc.log(LOG_INFO, "MigrationMemoryManager: all blocks have been created");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::~MigrationMemoryManager
//------------------------------------------------------------------------------
MigrationMemoryManager::~MigrationMemoryManager() throw() {
  // Make sure the thread is finished: this should be done by the caller,
  // who should have called waitThreads.
  // castor::tape::threading::Thread::wait();
  // we expect to be called after all users are finished. Just "free"
  // the memory blocks we still have.
  castor::tape::threading::BlockingQueue<MemBlock*>::valueRemainingPair ret;
  do {
    ret = m_freeBlocks.popGetSize();
    delete ret.value;
  } while (ret.remaining > 0);

  m_lc.log(LOG_INFO, "MigrationMemoryManager destruction : all memory blocks have been deleted");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::startThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::startThreads()  {
  castor::tape::threading::Thread::start();
  m_lc.log(LOG_INFO, "MigrationMemoryManager starting thread");
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::waitThreads
//------------------------------------------------------------------------------
void MigrationMemoryManager::waitThreads()  {
  castor::tape::threading::Thread::wait();
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
bool MigrationMemoryManager::areBlocksAllBack()
throw(){
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
  addClient(NULL);
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::releaseBlock
//------------------------------------------------------------------------------
void MigrationMemoryManager::releaseBlock(MemBlock* mb)
 {
  mb->reset();
  m_freeBlocks.push(mb);
  {
    castor::tape::threading::MutexLocker ml(&m_countersMutex);
    m_blocksReturned++;
  }
}

//------------------------------------------------------------------------------
// MigrationMemoryManager::run
//------------------------------------------------------------------------------
void MigrationMemoryManager::run()  {
  while (true) {
    DataPipeline* c = m_clientQueue.pop();
    // If the c is a NULL pointer, that means end of clients
    if (!c) return;
    // Spin on the the client. We rely on the fact that he will want
    // at least one block (which is the case currently)
    while (c->provideBlock(m_freeBlocks.pop())) {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      m_blocksProvided++;
    }
  }
}

}
}
}
}
