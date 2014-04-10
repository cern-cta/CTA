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
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include <memory>
namespace {
  
  uint32_t blockID(const castor::tape::tapegateway::FileToRecallStruct& ftr)
  {
    return (ftr.blockId0() << 24) | (ftr.blockId1() << 16) |  (ftr.blockId2() << 8) | ftr.blockId3();
  }
  
  /*Use RAII to make sure the memory block is released  
   *(ie pushed back to the memory manager) in any case (exception or not)
   */
  class AutoReleaseBlock{
    castor::tape::tapeserver::daemon::MemBlock *block;
    castor::tape::tapeserver::daemon::MemoryManager& memManager;
  public:
    AutoReleaseBlock(castor::tape::tapeserver::daemon::MemBlock* mb, 
            castor::tape::tapeserver::daemon::MemoryManager& mm):
    block(mb),memManager(mm){}
    
    ~AutoReleaseBlock(){
      memManager.releaseBlock(block);
    }
  };
}
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
   * @param file: All we need to know about the file we  are recalling
   * @param mm: memory manager of the session
   */
  DiskWriteTask(tape::tapegateway::FileToRecallStruct* file,MemoryManager& mm): 
  m_recallingFile(file),m_memManager(mm){
 
  }
  /**
   * Main routine: takes each memory block in the fifo and writes it to disk
   * @return true if the file has been successfully written false otherwise.
   */
  virtual bool execute(ReportPackerInterface<detail::Recall>& reporter,log::LogContext& lc) {
    using log::LogContext;
    using log::Param;
    try{
      tape::diskFile::WriteFile ourFile(m_recallingFile->path());
      int blockId  = 0;
      
      MemBlock *mb = m_fifo.pop();
      while(NULL!=mb){
        
        AutoReleaseBlock releaser(mb,m_memManager);
        
        if(m_recallingFile->fileid() != static_cast<unsigned int>(mb->m_fileid)
                || blockId != mb->m_fileBlock  || mb->m_failled ){
          LogContext::ScopedParam sp[]={
            LogContext::ScopedParam(lc, Param("expected_NSFILEID",m_recallingFile->fileid())),
            LogContext::ScopedParam(lc, Param("received_NSFILEID", mb->m_fileid)),
            LogContext::ScopedParam(lc, Param("expected_NSFBLOCKId", blockId)),
            LogContext::ScopedParam(lc, Param("received_NSFBLOCKId", mb->m_fileBlock)),
            LogContext::ScopedParam(lc, Param("failed_Status", mb->m_failled))
          };
          tape::utils::suppresUnusedVariable(sp);
          lc.log(LOG_ERR,"received a bad block for writing");
          throw castor::tape::Exception("received a bad block for writing");
        }
        mb->m_payload.write(ourFile);
        mb = m_fifo.pop();
        blockId++;
      }
      ourFile.close();
      reporter.reportCompletedJob(*m_recallingFile);
      return true;
    }
    catch(const castor::exception::Exception& e){
      /*
       *We might end up there with some blocks into m_fifo
       * We need to empty it
       */
      releaseAllBlock();
      
      reporter.reportFailedJob(*m_recallingFile,e.getMessageValue(),e.code());
      return false;
    }
  }
  
  /**
   * Allows client code to return a reusable memory block. Should not been called
   * @return the pointer to the memory block that can be reused
   */
  virtual MemBlock *getFreeBlock() { 
    throw castor::tape::Exception("DiskWriteTask::getFreeBlock should mot be called");
  }
  
  /**
   * Function used to enqueue a new memory block holding data to be written to disk
   * @param mb: corresponding memory block
   */
  virtual void pushDataBlock(MemBlock *mb) {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_fifo.push(mb);
  }

  /**
   * Destructor (also waiting for the end of the write operation)
   */
  virtual ~DiskWriteTask() { 
    volatile castor::tape::threading::MutexLocker ml(&m_producerProtection); 
  }
  
private:

  void releaseAllBlock(){
      try
      {
        while(1){
          MemBlock* mb=m_fifo.tryPop();
          if(mb) {
            AutoReleaseBlock(mb,m_memManager);
          }
        }
      }
      catch(const castor::tape::threading::noMore&){}   
  }
  /**
   * The fifo containing the memory blocks holding data to be written to disk
   */
  castor::tape::threading::BlockingQueue<MemBlock *> m_fifo;
  /** 
   * All we need to know about the file we are currently recalling
   */
  std::auto_ptr<tape::tapegateway::FileToRecallStruct> m_recallingFile;
    
  /**
   * Reference to the Memory Manager in use
   */
  MemoryManager & m_memManager;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  castor::tape::threading::Mutex m_producerProtection;
  
};

}}}}
