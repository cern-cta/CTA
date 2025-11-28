/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "common/process/threading/BlockingQueue.hpp"
#include "common/process/threading/Thread.hpp"

namespace castor {
namespace exception {
class Exception;
}

namespace tape::tapeserver::daemon {

class TapeWriteTask;
class MemBlock;
class DataPipeline;

/**
 * The memory manager is responsible for allocating memory blocks and distributing
 * the free ones around to any class in need. The distribution is actively run in
 * a thread.
 */
class MigrationMemoryManager : private cta::threading::Thread {
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
  size_t blockCapacity() const;

  /**
   * Are all sheep back to the farm?
   * @return
   */
  bool areBlocksAllBack() const noexcept;

  /**
   * Start serving clients (in the dedicated thread)
   */
  void startThreads();

  /**
   * Waiting for clients to finish (in the dedicated thread)
   */
  void waitThreads();

  /**
   * Adds a new client in need for free memory blocks
   * @param c: the new client
   */
  void addClient(DataPipeline* c);

  /**
   * Takes back a block which has been released by one of the clients
   * @param mb: the pointer to the block
   */
  void releaseBlock(std::unique_ptr<MemBlock> mb) ;

  /**
   * Function used to specify that there are no more clients for this memory manager.
   * See the definition of endOfClients below.
   */
  void finish();

  /**
   * Get the total number of bytes allocated.
   * @return the total number of bytes allocated.
   */
  size_t getTotalMemoryAllocated() const;

  /**
   * Finds the number of bytes in use by looking how many blocks are in use.
   * @return the number of bytes in use
   */
  size_t getTotalMemoryUsed() const;

  /**
   * Destructor
   */
  ~MigrationMemoryManager() noexcept override;

private:
  /**
   * Number of bytes allocated per block
   */
  const size_t m_blockCapacity;

  /**
   * Total number of allocated memory blocks
   */
  size_t m_totalNumberOfBlocks = 0;

  /**
   * Total amount of memory allocated
   */
  size_t m_totalMemoryAllocated = 0;

  /**
   * Container for the free blocks
   */
  cta::threading::BlockingQueue<std::unique_ptr<MemBlock>> m_freeBlocks;

  /**
   * The client queue: we will feed them as soon as blocks
   * become free. This is done in a dedicated thread.
   */
  cta::threading::BlockingQueue<DataPipeline*> m_clientQueue;

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

}  // namespace tape::tapeserver::daemon
}  // namespace castor
