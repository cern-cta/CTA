/******************************************************************************
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

#include "castor/tape/tapeserver/daemon/DiskWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/AutoReleaseBlock.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/log/LogContext.hpp"
namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskWriteTask::DiskWriteTask(tape::tapegateway::FileToRecallStruct* file,RecallMemoryManager& mm): 
m_recallingFile(file),m_memManager(mm){

}

//------------------------------------------------------------------------------
// DiskWriteTask::execute
//------------------------------------------------------------------------------
bool DiskWriteTask::execute(RecallReportPacker& reporter,log::LogContext& lc) {
  using log::LogContext;
  using log::Param;
  try{
    tape::diskFile::WriteFile ourFile(m_recallingFile->path());
    int blockId  = 0;
    unsigned long checksum = Payload::zeroAdler32();
    while(1) {
      if(MemBlock* const mb = m_fifo.pop()) {
        AutoReleaseBlock<RecallMemoryManager> releaser(mb,m_memManager);
        if(mb->isCanceled()) {
          // If the tape side got canceled, we report nothing and count
          // it as a success.
          lc.log(LOG_DEBUG, "File transfer canceled");
          return true;
        }
        checkErrors(mb,blockId,lc);
        //----------------------------end deal with errors-----------------------
        mb->m_payload.write(ourFile);
        checksum = mb->m_payload.adler32(checksum);
        blockId++;
      } //end if block non NULL
      else {        
        break;
      }
    } //end of while(1)
    lc.log(LOG_DEBUG, "File successfully transfered.");
    reporter.reportCompletedJob(*m_recallingFile,checksum);
    return true;
  } //end of try
  catch(const castor::exception::Exception& e){
    /*
     *We might end up there because ;
     * -- WriteFile failed 
     * -- A desynchronization between tape read and disk write
     * -- An error in tape read
     * -- An error while writing the file
     */
    
    //there might still be some blocks into m_fifo
    // We need to empty it
    releaseAllBlock();
    
    reporter.reportFailedJob(*m_recallingFile,e.getMessageValue(),e.code());
    return false;
  }
}

//------------------------------------------------------------------------------
// DiskWriteTask::getFreeBlock
//------------------------------------------------------------------------------
MemBlock *DiskWriteTask::getFreeBlock() { 
  throw castor::tape::Exception("DiskWriteTask::getFreeBlock should mot be called");
}

//------------------------------------------------------------------------------
// DiskWriteTask::pushDataBlock
//------------------------------------------------------------------------------
void DiskWriteTask::pushDataBlock(MemBlock *mb) {
  castor::tape::threading::MutexLocker ml(&m_producerProtection);
  m_fifo.push(mb);
}

//------------------------------------------------------------------------------
// DiskWriteTask::~DiskWriteTask
//------------------------------------------------------------------------------
DiskWriteTask::~DiskWriteTask() { 
  volatile castor::tape::threading::MutexLocker ml(&m_producerProtection); 
}

//------------------------------------------------------------------------------
// DiskWriteTask::releaseAllBlock
//------------------------------------------------------------------------------
void DiskWriteTask::releaseAllBlock(){
  while(1){
    if(MemBlock* mb=m_fifo.pop())
      AutoReleaseBlock<RecallMemoryManager> release(mb,m_memManager);
    else 
      break;
  }
}

//------------------------------------------------------------------------------
// checkErrors
//------------------------------------------------------------------------------  
  void DiskWriteTask::checkErrors(MemBlock* mb,int blockId,castor::log::LogContext& lc){
    using namespace castor::log;
    if(m_recallingFile->fileid() != static_cast<unsigned int>(mb->m_fileid)
            || blockId != mb->m_fileBlock  || mb->isFailed() ){
      LogContext::ScopedParam sp[]={
        LogContext::ScopedParam(lc, Param("received_NSFILEID", mb->m_fileid)),
        LogContext::ScopedParam(lc, Param("expected_NSFBLOCKId", blockId)),
        LogContext::ScopedParam(lc, Param("received_NSFBLOCKId", mb->m_fileBlock)),
        LogContext::ScopedParam(lc, Param("failed_Status", mb->isFailed()))
      };
      tape::utils::suppresUnusedVariable(sp);
      std::string errorMsg;
      int errCode;
      if(mb->isFailed()){
        errorMsg=mb->errorMsg();
        errCode=mb->errorCode();
      }
      else{
        errorMsg="Mistmatch between expected and received filed or blockid";
        errCode=SEINTERNAL;
      }
      lc.log(LOG_ERR,errorMsg);
      throw castor::exception::Exception(errCode,errorMsg);
    }
  }

}}}}

