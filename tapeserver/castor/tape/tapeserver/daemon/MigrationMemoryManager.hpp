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

#pragma once

#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/log/LogContext.hpp"

namespace castor {
namespace exception {
// Forward declaration  
class Exception;
}
namespace tape {
namespace tapeserver {
namespace daemon {
// Forward declaration
class TapeWriteTask;
class MemBlock;
class DataPipeline;
/**
 * The memory manager is responsible for allocating memory blocks and distributing
 * the free ones around to any class in need. The distribution is actively run in
 * a thread.
 */
class MigrationMemoryManager: private cta::threading::Thread {
public:
  
  /**
   * Constructor
   * @param numberOfBlocks: number of blocks to allocate
   * @param blockSize: size of each block
   */
  MigrationMemoryManager(const uint32_t numberOfBlocks, const uint32_t blockSize, const cta::log::LogContext& lc);
  
  /**
   * 
   * @return the nominal capacity of one block 
   */
  size_t blockCapacity();
  
  /**
   * Are all sheep back to the farm?
   * @return 
   */
  bool areBlocksAllBack() noexcept;
  
  /**
   * Start serving clients (in the dedicated thread)
   */
  void startThreads() ;
  
  /**
   * Waiting for clients to finish (in the dedicated thread)
   */
  void waitThreads() ;
  
  /**
   * Adds a new client in need for free memory blocks
   * @param c: the new client
   */
  void addClient(DataPipeline* c) ;
  
  /**
   * Takes back a block which has been released by one of the clients
   * @param mb: the pointer to the block
   */
  void releaseBlock(MemBlock *mb) ;
  
  /**
   * Function used to specify that there are no more clients for this memory manager.
   * See the definition of endOfClients below.
   */
  void finish() ;

  /**
   * Destructor
   */
  ~MigrationMemoryManager() noexcept override;

private:
  const size_t m_blockCapacity;
  
  /**
   * Total number of allocated memory blocks
   */
  size_t m_totalNumberOfBlocks;
  
  /**
   * Total amount of memory allocated
   */
  size_t m_totalMemoryAllocated;
  
  /**
   * Count of blocks provided
   */
  size_t m_blocksProvided;
  
  /**
   * Count of blocks returned
   */
  size_t m_blocksReturned;
  
  /**
   * Mutex protecting the counters
   */
  cta::threading::Mutex m_countersMutex;
  
  /**
   * Container for the free blocks
   */
  cta::threading::BlockingQueue<MemBlock *> m_freeBlocks;
  
  /**
   * The client queue: we will feed them as soon as blocks
   * become free. This is done in a dedicated thread.
   */
  cta::threading::BlockingQueue<DataPipeline *> m_clientQueue;

  /**
   * Logging purpose. Given the fact the class is threaded, the LogContext
   * has to be copied.
   */
  cta::log::LogContext m_lc;
    
  /**
   * Thread routine: pops a client and provides him blocks until he is happy!
   */
  void run() override;
  
};

}
}
}
}

