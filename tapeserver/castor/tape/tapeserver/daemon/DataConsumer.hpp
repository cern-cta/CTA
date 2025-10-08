/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace castor::tape::tapeserver::daemon {

class MemBlock;

/**
 * Abstract class used as a base class for the disk/tape write file tasks. The data consumer
 * has two methods: "pushDataBlock" used to put in the consumer's fifo a new full memory block
 * to consume, and "getFreeBlock" used by client code two reclaim the consumed memory block.
 */
class DataConsumer {
public:

  /**
   * Returns used (consumed) memory blocks.
   * @return the memory block to be reclaimed
   */
  virtual MemBlock* getFreeBlock() = 0;

  /**
   * Inserts a new memory block in the consumers fifo.
   * @param mb memory block to be inserted in the consumer fifo and consumed
   */
  virtual void pushDataBlock(MemBlock *mb) = 0;

  /**
   * Destructor
   */
  virtual ~DataConsumer() = default;
};

} // namespace castor::tape::tapeserver::daemon
