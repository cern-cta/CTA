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
