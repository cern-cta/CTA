/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/log/LogContext.hpp"

namespace castor {
namespace exception {
class Exception;
}

namespace tape::tapeserver::daemon {

class MemBlock;

/**
 * The memory manager is responsible for allocating memory blocks and distributing
 * the free ones around to any class in need.
 */
class RecallMemoryManager {
public:
  /**
   * Constructor
   *
   * @param numberOfBlocks  number of blocks to allocate
   * @param blockSize       size of each block
   */
  RecallMemoryManager(size_t numberOfBlocks, size_t blockSize, cta::log::LogContext& lc);

  /**
   * Are all sheep back to the farm?
   * @return
   */
  bool areBlocksAllBack() noexcept;

  /**
   * Takes back a block which has been released by one of the clients
   * @param mb: the pointer to the block
   */
  void releaseBlock(MemBlock *mb);

  /**
   * Pop a free block from the free block queue of the memory manager
   * @return pointer to a free block
   */
  MemBlock* getFreeBlock();

  /**
   * Destructor
   */
  ~RecallMemoryManager();

private:
  /**
   * Total number of allocated memory blocks
   */
  size_t m_totalNumberOfBlocks;

  /**
   * Container for the free blocks
   */
  cta::threading::BlockingQueue<MemBlock*> m_freeBlocks;

  /**
   * Logging. The class is not threaded, so it can be shared with its parent
   */
  cta::log::LogContext& m_lc;
};

}} // namespace castor::tape::tapeserver::daemon
