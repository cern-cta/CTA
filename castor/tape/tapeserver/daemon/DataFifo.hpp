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
#include "castor/tape/tapeserver/daemon/Exception.hpp"
#include "castor/exception/Exception.hpp"

/* A fixed payload FIFO: at creation time, we know how many blocks will go through the FIFO.
  The provide block method return true as long as it still needs more block. False when last
  block is provided (and throws an exception after that).
 */
class DataFifo : public MemoryManagerClient {
public:

  DataFifo(int bn) throw() : m_blocksNeeded(bn), m_freeBlocksProvided(0),
  m_dataBlocksPushed(0), m_dataBlocksPopped(0) {};
  
  ~DataFifo() throw() { castor::tape::threading::MutexLocker ml(&m_freeBlockProviderProtection); }

  /* Memory manager client interface implementation */
  virtual bool provideBlock(MemBlock *mb) throw(MemException) {
    bool ret;
    castor::tape::threading::MutexLocker ml(&m_freeBlockProviderProtection);
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      if (m_freeBlocksProvided >= m_blocksNeeded)
        throw MemException("DataFifo overflow on free blocks");
      m_freeBlocksProvided++;
      ret = m_freeBlocksProvided < m_blocksNeeded;
    }
    m_freeBlocks.push(mb);
    return ret;
  }
  virtual bool endOfWork() throw() { return false; }

  /* Rest of the data Fifo interface. */
  MemBlock * getFreeBlock()  throw(castor::exception::Exception) {
    return m_freeBlocks.pop();
  }

  void pushDataBlock(MemBlock *mb) throw(castor::exception::Exception) {
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      if (m_dataBlocksPushed >= m_blocksNeeded)
        throw MemException("DataFifo overflow on data blocks");
    }
    m_dataBlocks.push(mb);
    {
        castor::tape::threading::MutexLocker ml(&m_countersMutex);
        m_dataBlocksPushed++;
    }
  }

  MemBlock * popDataBlock() throw(castor::exception::Exception) {
    MemBlock *ret = m_dataBlocks.pop();
    {
      castor::tape::threading::MutexLocker ml(&m_countersMutex);
      m_dataBlocksPopped++;
    }
    return ret;
  }

  bool finished() throw() {
    // No need to lock because only one int variable is read.
    //TODO : are we sure the operation is atomic ? It is plateform dependant
    return m_dataBlocksPopped >= m_blocksNeeded;
  }
  
private:
  castor::tape::threading::Mutex m_countersMutex;
  castor::tape::threading::Mutex m_freeBlockProviderProtection;
  int m_blocksNeeded;
  volatile int m_freeBlocksProvided;
  volatile int m_dataBlocksPushed;
  volatile int m_dataBlocksPopped;
  castor::tape::threading::BlockingQueue<MemBlock *> m_freeBlocks;
  castor::tape::threading::BlockingQueue<MemBlock *> m_dataBlocks;
};
