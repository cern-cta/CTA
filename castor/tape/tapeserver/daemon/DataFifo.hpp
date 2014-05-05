/******************************************************************************
 *                      DataFifo.hpp
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

#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/tape/tapeserver/daemon/MemManagerClient.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/* A fixed payload FIFO: at creation time, we know how many blocks will go 
 * through the FIFO (its size). The provide block method return true as long as 
 * it still needs more block. False when last block is provided (and throws an 
 * exception after that).

 * Sum up                                                                               
                                          
                +------------------------------+                |
    getFreeBlock|                              |  provideBlock  |
     <----------+                              <----------------+   
                |      DataFifo                |              
                |                              |              
     +---------->                              +--------------->   
  pushDataBlock +------------------------------+  getDataBlock 
                                                                 
 */
class DataFifo : public MemoryManagerClient {
public:
 /**
  * Constructor
  * @param bn :how many memory block we want in the fifo (its size)
  */
  DataFifo(int bn)  : m_blocksNeeded(bn), m_freeBlocksProvided(0),
  m_dataBlocksPushed(0), m_dataBlocksPopped(0) {};
  
  ~DataFifo() throw() { 
    castor::tape::threading::MutexLocker ml(&m_freeBlockProviderProtection); 
  }

  /* 
   * Return a memory block to the object
   * @param mb : the memory block to be returned 
   * @return true   true if not all the needed blocks has not yet been provided 
   */
  virtual bool provideBlock(MemBlock *mb)  {
    bool ret;
    castor::tape::threading::MutexLocker ml(&m_freeBlockProviderProtection);
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      if (m_freeBlocksProvided >= m_blocksNeeded)
        throw castor::tape::exceptions::MemException("DataFifo overflow on free blocks");
      m_freeBlocksProvided++;
      ret = m_freeBlocksProvided < m_blocksNeeded;
    }
    m_freeBlocks.push(mb);
    return ret;
  }
  
  /* 
   * Get a free block 
   * @return a free block
   */
  MemBlock * getFreeBlock() {
    return m_freeBlocks.pop();
  }

  /**
   * Push into the object a memory block that has been filled somehow  :
   * tape/disk reading
   * @param mb the block we want to push back 
   */
  void pushDataBlock(MemBlock *mb)  {
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      if (m_dataBlocksPushed >= m_blocksNeeded)
        throw castor::tape::exceptions::MemException("DataFifo overflow on data blocks");
    }
    m_dataBlocks.push(mb);
    {
        castor::tape::threading::MutexLocker ml(&m_countersMutex);
        m_dataBlocksPushed++;
    }
  }

  /**
   * Push into the object a memory block that has been filled somehow  :
   * tape/disk reading
   * @param mb the block we want to push back 
   */
  MemBlock * popDataBlock() {
    MemBlock *ret = m_dataBlocks.pop();
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      m_dataBlocksPopped++;
    }
    return ret;
  }

  /**
   * Check if we have finish
   * @return Return true if we have popped more data blocks than its size
   */
  bool finished() {
    // No need to lock because only one int variable is read.
    //TODO : are we sure the operation is atomic ? It is plateform dependant
    castor::tape::threading::MutexLocker ml(&m_countersMutex);
    return m_dataBlocksPopped >= m_blocksNeeded;
  }
  
private:
  castor::tape::threading::Mutex m_countersMutex;
  castor::tape::threading::Mutex m_freeBlockProviderProtection;
  
  ///the number of memory blocks we want to be provided to the object (its size).
  const int m_blocksNeeded;
  
  ///how many blocks have been currently provided 
  volatile int m_freeBlocksProvided;
  
  ///how many data blocks have been currently pushed
  volatile int m_dataBlocksPushed;

  ///how many data blocks have been currently taken
  volatile int m_dataBlocksPopped;
  
    ///thread sage storage of all free blocks
  castor::tape::threading::BlockingQueue<MemBlock *> m_freeBlocks;
  
  ///thread sage storage of all blocks filled with data
  castor::tape::threading::BlockingQueue<MemBlock *> m_dataBlocks;
};

}
}
}
}
