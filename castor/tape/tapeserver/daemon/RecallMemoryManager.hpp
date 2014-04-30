/******************************************************************************
 *                      RecallMemoryManager.hpp
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


#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/tape/tapeserver/daemon/MemManagerClient.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/exception/Exception.hpp"
#include <iostream>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
/**
 * The memory manager is responsible for allocating memory blocks and distributing
 * the free ones around to any class in need.
 */
class RecallMemoryManager {
public:
  
  /**
   * Constructor
   * @param numberOfBlocks: number of blocks to allocate
   * @param blockSize: size of each block
   */
  RecallMemoryManager(const size_t numberOfBlocks, const size_t blockSize,
          castor::log::LogContext& lc) 
  : m_totalNumberOfBlocks(numberOfBlocks),m_lc(lc) {
    for (size_t i = 0; i < numberOfBlocks; i++) {
      m_freeBlocks.push(new MemBlock(i, blockSize));
      
      m_lc.pushOrReplace(log::Param("blockId",i));
      m_lc.log(LOG_DEBUG,"RecallMemoryManager created a block");
    }
    m_lc.log(LOG_INFO,"RecallMemoryManager: all blocks have been created");
  }
  
  /**
   * Are all sheep back to the farm?
   * @return 
   */
  bool areBlocksAllBack() throw() {
    return m_totalNumberOfBlocks==m_freeBlocks.size();
  }
  
  /**
   * Takes back a block which has been released by one of the clients
   * @param mb: the pointer to the block
   */
  void releaseBlock(MemBlock *mb)  {
    m_lc.pushOrReplace(log::Param("blockId",mb->m_memoryBlockId));
    m_lc.log(LOG_DEBUG,"RecallMemoryManager A block has been released");
    mb->reset();
    m_freeBlocks.push(mb);
  }
  
  MemBlock* getFreeBlock(){
    return m_freeBlocks.pop();
  }
  
  /**
   * Destructor
   */
  ~RecallMemoryManager() {
    // Make sure the thread is finished: this should be done by the caller,
    // who should have called waitThreads.
    // castor::tape::threading::Thread::wait();
    // we expect to be called after all users are finished. Just "free"
    // the memory blocks we still have.
    
    castor::tape::threading::BlockingQueue<MemBlock*>::valueRemainingPair ret;
    do{
      ret=m_freeBlocks.popGetSize();
      delete ret.value;
    }while(ret.remaining>0);
    
    m_lc.log(LOG_INFO,"RecallMemoryManager destruction : all memory blocks have been deleted");
  }
  
private:
  
  
  /**
   * Total number of allocated memory blocks
   */
  size_t m_totalNumberOfBlocks;
  
  /**
   * Container for the free blocks
   */
  castor::tape::threading::BlockingQueue<MemBlock*> m_freeBlocks;
  
  /**
   * Logging. The class is not threaded, so it can be shared with its parent
   */
  castor::log::LogContext& m_lc;
};

}}}}

