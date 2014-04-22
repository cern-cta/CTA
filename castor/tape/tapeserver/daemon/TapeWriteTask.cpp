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
#include "castor/tape/tapeserver/file/File.hpp" 

namespace {  
   unsigned long initAdler32Checksum() {
     return  adler32(0L,Z_NULL,0);
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


  TapeWriteTask::TapeWriteTask(int blockCount, tapegateway::FileToMigrateStruct* file,MemoryManager& mm): 
  m_fileToMigrate(file),m_memManager(mm), m_fifo(blockCount),m_blockCount(blockCount)
  {
    mm.addClient(&m_fifo); 
  }

   int TapeWriteTask::blocks() { 
    return m_blockCount; 
  }
  
   void TapeWriteTask::execute(castor::tape::tapeFile::WriteSession & session,
           MigrationReportPacker & reportPacker,castor::log::LogContext& lc) {
    using castor::log::LogContext;
    using castor::log::Param;
    
    unsigned long ckSum = initAdler32Checksum();
    int blockId  = 0;

    try {
      std::auto_ptr<castor::tape::tapeFile::WriteFile> output(openWriteFile(session,lc));
      while(!m_fifo.finished()) {
        MemBlock* const mb = m_fifo.popDataBlock();
        AutoReleaseBlock releaser(mb,m_memManager);
        
        if(/*m_migratingFile->fileid() != static_cast<unsigned int>(mb->m_fileid)
            *             || */blockId != mb->m_fileBlock  || mb->m_failed ){
          LogContext::ScopedParam sp[]={
            LogContext::ScopedParam(lc, Param("received_NSFILEID", mb->m_fileid)),
            LogContext::ScopedParam(lc, Param("expected_NSFBLOCKId", blockId)),
            LogContext::ScopedParam(lc, Param("received_NSFBLOCKId", mb->m_fileBlock)),
            LogContext::ScopedParam(lc, Param("failed_Status", mb->m_failed))
          };
          tape::utils::suppresUnusedVariable(sp);
          lc.log(LOG_ERR,"received a bad block for writing");
          throw castor::tape::Exception("received a bad block for writing");
        }
        ckSum =  mb->m_payload.adler32(ckSum);
        mb->m_payload.write(*output);
        ++blockId;
      }
      reportPacker.reportCompletedJob(*m_fileToMigrate,ckSum);
    }
    catch(const castor::tape::Exception& e){
      //we can end up there because
      //we failed to open the WriteFile
      //we received a bad block or a block written failed
      lc.log(LOG_ERR,"Circulating blocks into TapeWriteTask::execute");
      while(!m_fifo.finished()) {
        m_memManager.releaseBlock(m_fifo.popDataBlock());
      }
      reportPacker.reportFailedJob(*m_fileToMigrate,e.getMessageValue(),e.code());
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

   std::auto_ptr<tapeFile::WriteFile> TapeWriteTask::openWriteFile(
   tape::tapeFile::WriteSession & session, log::LogContext& lc){
      std::auto_ptr<tape::tapeFile::WriteFile> output;
      try{
        output.reset(new tape::tapeFile::WriteFile(&session, *m_fileToMigrate,m_memManager.blockCapacity()));
        lc.log(LOG_DEBUG, "Successfully opened the tape file for writing");
      }
      catch(const castor::exception::Exception & ex){
        lc.log(LOG_ERR, "Failed to open tape file for writing");
        throw;
      }
      return output;
    }
}}}}


