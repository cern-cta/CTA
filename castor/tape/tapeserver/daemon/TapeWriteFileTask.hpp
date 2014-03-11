/******************************************************************************
 *                      TapeWriteFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"

/**
 * The TapeWriteFileTask is responsible to write a single file onto tape as part of a migration
 * session. Being a consumer of memory blocks, it inherits from the DataConsumer class. It also
 * inherits several methods from the TapeWriteTask (TODO: do we really need this base class?).
 */
class TapeWriteFileTask: public TapeWriteTask, public DataConsumer {
public:
  /**
   * Constructor
   * @param fSeq: file sequence number of the file to be written on tape
   * @param blockCount: number of memory blocks (TODO:?)
   * @param mm: reference to the memory manager in use
   */
  TapeWriteFileTask(int fSeq, int blockCount, MemoryManager& mm): m_fSeq(fSeq),
          m_memManager(mm), m_fifo(blockCount), m_blockCount(blockCount)
  { mm.addClient(&m_fifo); }
  
  /**
   * TODO: see other comments
   * @return always false
   */
  virtual bool endOfWork() { return false; }
  
  /**
   * Returns the number of files to be written to tape
   * @return always 1
   */
  virtual int files() { return 1; }
  
  /**
   * @return the number of memory blocks to be used
   */
  virtual int blocks() { return m_blockCount; }
  
  /**
   * @return the file sequence number of the file to be written on tape
   */
  virtual int fSeq() { return m_fSeq; }
  
  /**
   * Main execution routine
   * @param td: tape drive object which will handle the file
   */
  virtual void execute(castor::tape::drives::DriveInterface & td) {
    int blockId  = 0;
    while(!m_fifo.finished()) {
      MemBlock *mb = m_fifo.popDataBlock();
      mb->m_fSeq = m_fSeq;
      mb->m_tapeFileBlock = blockId++;
      //td.writeBlock(mb);
      m_memManager.releaseBlock(mb);
    }
  }
  
  /**
   * Used to reclaim used memory blocks
   * @return the recyclable memory block
   */
  virtual MemBlock * getFreeBlock() { return m_fifo.getFreeBlock(); }
  
  /**
   * This is to enqueue one memory block full of data to be written on tape
   * @param mb: the memory block in question
   */
  virtual void pushDataBlock(MemBlock *mb) {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_fifo.pushDataBlock(mb);
  }
  
  /**
   * Destructor
   */
  virtual ~TapeWriteFileTask() { castor::tape::threading::MutexLocker ml(&m_producerProtection); }
  
private:
  
  /**
   * The file sequence number of the file to be written on tape
   */
  int m_fSeq;
  
  /**
   * reference to the memory manager in use   
   */
  MemoryManager & m_memManager;
  
  /**
   * The fifo containing the memory blocks holding data to be written to tape
   */
  DataFifo m_fifo;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  castor::tape::threading::Mutex m_producerProtection;
  
  /**
   * The number of memory blocks to be used
   */
  int m_blockCount;
};
