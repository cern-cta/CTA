/******************************************************************************
 *                      TapeWriteFileTask.cpp
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


#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/DataFifo.hpp"
#include "castor/tape/tapeserver/daemon/MemManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"

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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {


  TapeWriteTask::TapeWriteTask(int fSeq, int blockCount, MemoryManager& mm): 
  m_fSeq(fSeq),m_memManager(mm), m_fifo(blockCount), m_blockCount(blockCount)
  {
    mm.addClient(&m_fifo); 
  }

   int TapeWriteTask::blocks() { 
    return m_blockCount; 
  }
  

   int TapeWriteTask::fSeq() { 
    return m_fSeq; 
  }
  

   void TapeWriteTask::execute(castor::tape::drives::DriveInterface & td,castor::log::LogContext& lc) {
    using castor::log::LogContext;
    using castor::log::Param;
    
    int blockId  = 0;
    while(!m_fifo.finished()) {
      MemBlock* const mb = m_fifo.popDataBlock();
      
      if(/*m_migratingFile->fileid() != static_cast<unsigned int>(mb->m_fileid)
              || */blockId != mb->m_fileBlock  || mb->m_failled ){
        LogContext::ScopedParam sp[]={
          //LogContext::ScopedParam(lc, Param("expected_NSFILEID",m_recallingFile->fileid())),
          LogContext::ScopedParam(lc, Param("received_NSFILEID", mb->m_fileid)),
          LogContext::ScopedParam(lc, Param("expected_NSFBLOCKId", blockId)),
          LogContext::ScopedParam(lc, Param("received_NSFBLOCKId", mb->m_fileBlock)),
          LogContext::ScopedParam(lc, Param("failed_Status", mb->m_failled))
        };
        tape::utils::suppresUnusedVariable(sp);
        lc.log(LOG_ERR,"received a bad block for writing");
        throw castor::tape::Exception("received a bad block for writing");
      }
      //mb->m_payload.write(td);
      m_memManager.releaseBlock(mb);
      ++blockId;
    }
  }
  

  MemBlock * TapeWriteTask::getFreeBlock() { 
    return m_fifo.getFreeBlock(); 
  }
  

   void TapeWriteTask::pushDataBlock(MemBlock *mb) {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
    m_fifo.pushDataBlock(mb);
  }
  

   TapeWriteTask::~TapeWriteTask() {
    castor::tape::threading::MutexLocker ml(&m_producerProtection);
  }

}}}}


