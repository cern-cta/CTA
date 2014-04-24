/******************************************************************************
 *                      MemManager.hpp
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
 * the free ones around to any class in need. The distribution is actively run in
 * a thread.
 */
class MemoryManager: private castor::tape::threading::Thread {
public:
  
  /**
   * Constructor
   * @param numberOfBlocks: number of blocks to allocate
   * @param blockSize: size of each block
   */
  MemoryManager(const size_t numberOfBlocks, const size_t blockSize) 
          throw(castor::exception::Exception) : 
  m_blockCapacity(blockSize), m_totalNumberOfBlocks(0),
  m_totalMemoryAllocated(0), m_blocksProvided(0), m_blocksReturned(0) {
    for (size_t i = 0; i < numberOfBlocks; i++) {
      m_freeBlocks.push(new MemBlock(i, blockSize));
      m_totalNumberOfBlocks++;
      m_totalMemoryAllocated+=blockSize;
    }
  }
  
  /**
   * 
   * @return the nominal capacity of one block 
   */
  size_t blockCapacity(){
    return m_blockCapacity;
  }
  
  /**
   * Are all sheep back to the farm?
   * @return 
   */
  bool areBlocksAllBack() throw() {
    return m_totalNumberOfBlocks==m_freeBlocks.size();
  }
  
  /**
   * Start serving clients (in the dedicated thread)
   */
  void startThreads() throw(castor::exception::Exception) {
    castor::tape::threading::Thread::start();
  }
  
  /**
   * Waiting for clients to finish (in the dedicated thread)
   */
  void waitThreads() throw(castor::exception::Exception) {
    castor::tape::threading::Thread::wait();
  }
  
  /**
   * Adds a new client in need for free memory blocks
   * @param c: the new client
   */
  void addClient(MemoryManagerClient* c) throw(castor::exception::Exception) {
    m_clientQueue.push(c);
  }
  
  /**
   * Takes back a block which has been released by one of the clients
   * @param mb: the pointer to the block
   */
  void releaseBlock(MemBlock *mb) throw(castor::exception::Exception) {
    mb->reset();
    m_freeBlocks.push(mb);
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      m_blocksReturned++;
    }
  }
  
  /**
   * Function used to specify that there are no more clients for this memory manager.
   * See the definition of endOfClients below.
   */
  void finish() throw(castor::exception::Exception) {
    addClient(NULL);
  }

  /**
   * Destructor
   */
  ~MemoryManager() throw() {
    // Make sure the thread is finished: this should be done by the caller,
    // who should have called waitThreads.
    // castor::tape::threading::Thread::wait();
    // we expect to be called after all users are finished. Just "free"
    // the memory blocks we still have.
    try {
      while(true) {
        delete m_freeBlocks.tryPop();
      }
    }
    catch (castor::tape::threading::noMore) {
      //done
    } 
  }
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
  castor::tape::threading::Mutex m_countersMutex;
  
  /**
   * Container for the free blocks
   */
  castor::tape::threading::BlockingQueue<MemBlock *> m_freeBlocks;
  
  /**
   * The client queue: we will feed them as soon as blocks
   * become free. This is done in a dedicated thread.
   */
   castor::tape::threading::BlockingQueue<MemoryManagerClient *> m_clientQueue;

  /**
   * Thread routine: pops a client and provides him blocks until he is happy!
   */
  void run() throw(castor::exception::Exception) {
    while(true) {
      MemoryManagerClient* c = m_clientQueue.pop();
    
      // If the c is a NULL pointer, that means end of client 
      if (!c) {
        return;
      };
      
      /* Spin on the the client. We rely on the fact that he will want
       at least one block (which is the case currently) */
      while (c->provideBlock(m_freeBlocks.pop())) {
        {
          castor::tape::threading::MutexLocker ml (&m_countersMutex);
          m_blocksProvided++;
        }
        
      }
        
    }
  }
  
};

}
}
}
}

