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

/**
 * Abstract class used as a base class for the disk/tape write file tasks. The data consumer
 * has two methods: "pushDataBlock" used to put in the consumer's fifo a new full memory block
 * to consume, and "getFreeBlock" used by client code two reclaim the consumed memory block.
 */
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
// Antcipated declaration to hasten compilation
class MemBlock;

class DataConsumer {
public:
  
  /**
   * Returns used (consumed) memory blocks.
   * @return the memory block to be reclaimed
   */
  virtual MemBlock * getFreeBlock() = 0;
  
  /**
   * Inserts a new memory block in the consumers fifo.
   * @param mb memory block to be inserted in the consumer fifo and consumed
   */
  virtual void pushDataBlock(MemBlock *mb) = 0;
  
  /**
   * Destructor
   */
  virtual ~DataConsumer() {}
};

}
}
}
}
