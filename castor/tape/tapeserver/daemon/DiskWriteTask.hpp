/******************************************************************************
 *                      DiskWriteFileTask.hpp
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

#include "castor/tape/tapeserver/daemon/DiskWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
/**
 * The DiskWriteFileTask is responsible to write a single file onto disk as part of a recall
 * session. Being a consumer of memory blocks, it inherits from the DataConsumer class. It also
 * inherits several methods from the DiskWriteTask (TODO: do we really need this base class?).
 */
class DiskWriteTask: public DiskWriteTaskInterface, public DataConsumer {
public:
  /**
   * Constructor
   * @param fileId: file id of the file to write to disk
   * @param blockCount: number of memory blocks that will be used
   * @param mm: memory manager of the session
   */
  DiskWriteTask(int fileId, int blockCount, const std::string& filePath,MemoryManager& mm,RecallReportPacker& report): 
  m_fifo(blockCount),m_blockCount(blockCount), m_fileId(fileId), 
          m_memManager(mm),m_path(filePath),m_reporter(report){
    mm.addClient(&m_fifo); 
  }
  
  /**
   * Return the numebr of files to write to disk
   * @return always 1
   */
  virtual int files() { return 1; };
  
  /**
   * @return the number of memory blocks to be used
   */
  virtual int blocks() { return m_blockCount; }
  
  /**
   * Main routine: takes each memory block in the fifo and writes it to disk
   */
  virtual void execute() {
    int blockId  = 0;
    while(!m_fifo.finished()) {
      //printf("+++ In disk write file, id=%d\n",m_fileId);
      MemBlock *mb = m_fifo.popDataBlock();
      mb->m_fileid = m_fileId;
      mb->m_fileBlock = blockId++;
      m_writer.write(mb->m_payload.get(),mb->m_payload.size());
    }
  }
  
  /**
   * Allows client code to return a reusable memory block
   * @return the pointer to the memory block that can be reused
   */
  virtual MemBlock *getFreeBlock() { return m_fifo.getFreeBlock(); }
  
  /**
   * Function used to enqueue a new memory block holding data to be written to disk
   * @param mb: corresponding memory block
   */
  virtual void pushDataBlock(MemBlock *mb) {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_fifo.pushDataBlock(mb);
  }
  
  /**
   * Function used to wait until the end of the write
   */
  virtual void waitCompletion() { 
    volatile castor::tape::threading::MutexLocker ml(&m_producerProtection); 
  }
  
  /**
   * Destructor (also waiting for the end of the write operation)
   */
  virtual ~DiskWriteTask() { 
    volatile castor::tape::threading::MutexLocker ml(&m_producerProtection); 
  }
  
private:
  
  /**
   * The fifo containing the memory blocks holding data to be written to disk
   */
  DataFifo m_fifo;
  
  /**
   * Number of blocks in the fifo
   */
  const int m_blockCount;
  
  /**
   * File id of the file that will be written to disk
   */
  int m_fileId;
  
  /**
   * Reference to the Memory Manager in use
   */
  MemoryManager & m_memManager;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  castor::tape::threading::Mutex m_producerProtection;
  
  const std::string m_path;
  RecallReportPacker& m_reporter;
};

}}}}
