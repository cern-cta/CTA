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

#include "common/process/threading/BlockingQueue.hpp"
#include "common/process/threading/Thread.hpp"
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
   * Destructor
   */
  ~RecallMemoryManager();

  /**
   * Are all sheep back to the farm?
   * @return
   */
  bool areBlocksAllBack() const noexcept;

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
   * Get the total number of bytes allocated.
   * @return the total number of bytes allocated.
   */
  size_t getTotalMemoryAllocated() const;

  /**
   * Finds the number of bytes in use by looking how many blocks are in use.
   * @return the number of bytes in use
   */
  size_t getTotalMemoryUsed() const;

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
  cta::threading::BlockingQueue<MemBlock*> m_freeBlocks;

  /**
   * Logging. The class is not threaded, so it can be shared with its parent
   */
  cta::log::LogContext& m_lc;
};

}} // namespace castor::tape::tapeserver::daemon
