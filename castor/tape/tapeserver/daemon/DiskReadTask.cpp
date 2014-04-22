/******************************************************************************
 *                      DiskReadTask.cpp
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

#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/log/LogContext.hpp"
namespace{
  /*Use RAII to make sure the memory block is released  
   *(ie pushed back to the memory manager) in any case (exception or not)
   */
  class AutoPushBlock{
    castor::tape::tapeserver::daemon::MemBlock *block;
    castor::tape::tapeserver::daemon::DataConsumer& next;
  public:
    AutoPushBlock(castor::tape::tapeserver::daemon::MemBlock* mb, 
            castor::tape::tapeserver::daemon::DataConsumer& task):
    block(mb),next(task){}
    
    ~AutoPushBlock(){
      next.pushDataBlock(block);
    }
  };
}

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

  DiskReadTask::DiskReadTask(DataConsumer & destination, 
          tape::tapegateway::FileToMigrateStruct* file,size_t numberOfBlock):
  m_nextTask(destination),m_migratedFile(file),m_numberOfBlock(numberOfBlock) 
  {}

   void DiskReadTask::execute(log::LogContext& lc) {
    size_t blockId=0;
    size_t migratingFileSize=m_migratedFile->fileSize();
    try{
      tape::diskFile::ReadFile sourceFile(m_migratedFile->path());
      log::LogContext::ScopedParam sp(lc, log::Param("filePath",m_migratedFile->path()));
      lc.log(LOG_INFO,"Opened file on disk for migration ");
      while(migratingFileSize>0){
        MemBlock* const mb = m_nextTask.getFreeBlock();
        AutoPushBlock push(mb,m_nextTask);
        
        mb->m_fileid = m_migratedFile->fileid();
        mb->m_fileBlock = blockId++;
        
        migratingFileSize -= mb->m_payload.read(sourceFile);
        
        //we either read at full capacity (ie size=capacity) or if there different,
        //it should be the end => migratingFileSize should be 0. If it not, error
        if(mb->m_payload.size() != mb->m_payload.totalCapacity() && migratingFileSize>0){
          mb->m_failed=true;
          throw castor::tape::Exception("Error while reading a file. Did not read at full capacity but the file is not fully read");
        }
      } //end of while(migratingFileSize>0)
    }
    catch(const castor::tape::Exception& e){
      //we have to pump the blocks anyway, mark them failed and then pass them back to TapeWrite
      //Otherwise they would be stuck into TapeWriteTask free block fifo

      using log::LogContext;
      using log::Param;
      
      LogContext::ScopedParam sp(lc, Param("blockID",blockId));
      lc.log(LOG_ERR,e.getMessageValue());
      //deal here the number of mem block
      while(blockId<m_numberOfBlock) {
        MemBlock * mb = m_nextTask.getFreeBlock();
        mb->m_failed=true;
        m_nextTask.pushDataBlock(mb);
        ++blockId;
      } //end of while
    } //end of catch
  }

}}}}

