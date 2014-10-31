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

#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/DiskWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/AutoReleaseBlock.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/utils/Timer.hpp"

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
bool DiskWriteTask::execute(RecallReportPacker& reporter,log::LogContext& lc,
    diskFile::DiskFileFactory & fileFactory) {
  using log::LogContext;
  using log::Param;
  castor::utils::Timer localTime;
  castor::utils::Timer totalTime(localTime);
  castor::utils::Timer transferTime(localTime);
  try{
    // Placeholder for the disk file. We will open it only
    // after getting a first correct memory block.
    std::auto_ptr<tape::diskFile::WriteFile> writeFile;
    log::ScopedParamContainer URLcontext(lc);
    URLcontext.add("path", m_recallingFile->path());
    
    int blockId  = 0;
    unsigned long checksum = Payload::zeroAdler32();
    while(1) {
      if(MemBlock* const mb = m_fifo.pop()) {
        m_stats.waitDataTime+=localTime.secs(castor::utils::Timer::resetCounter);
        AutoReleaseBlock<RecallMemoryManager> releaser(mb,m_memManager);
        if(mb->isCanceled()) {
          // If the tape side got canceled, we report nothing and count
          // it as a success.
          lc.log(LOG_DEBUG, "File transfer canceled");
          return true;
        }
        
        //will throw (thus exiting the loop) if something is wrong
        checkErrors(mb,blockId,lc);
        m_stats.checkingErrorTime += localTime.secs(castor::utils::Timer::resetCounter);
        // If we got that far on the first pass, it's now good enough to open
        // the disk file for writing...
        if (!writeFile.get()) {
          lc.log(LOG_INFO, "About to open disk file for writing");
          transferTime.reset();
          writeFile.reset(fileFactory.createWriteFile(m_recallingFile->path()));
          URLcontext.add("actualURL", writeFile->URL());
          lc.log(LOG_INFO, "Opened disk file for writing");
          m_stats.openingTime+=localTime.secs(castor::utils::Timer::resetCounter);
        }
        
        // Write the data.
        m_stats.dataVolume+=mb->m_payload.size();
        mb->m_payload.write(*writeFile);
        m_stats.readWriteTime+=localTime.secs(castor::utils::Timer::resetCounter);
        
        checksum = mb->m_payload.adler32(checksum);
        m_stats.checksumingTime+=localTime.secs(castor::utils::Timer::resetCounter);
       
        blockId++;
      } //end if block non NULL
      else { 
        //close has to be explicit, because it may throw. 
        //A close is done  in WriteFile's destructor, but it may lead to some 
        //silent data loss
        writeFile->close();
        m_stats.closingTime +=localTime.secs(castor::utils::Timer::resetCounter);
        m_stats.filesCount++;
        break;
      }
    } //end of while(1)
    reporter.reportCompletedJob(*m_recallingFile,checksum,m_stats.dataVolume);
    m_stats.waitReportingTime+=localTime.secs(castor::utils::Timer::resetCounter);
    m_stats.transferTime = transferTime.secs();
    m_stats.totalTime = totalTime.secs();
    logWithStat(LOG_INFO, "File successfully transfered to disk",lc);
    
    //everything went well, return true
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
    m_stats.waitReportingTime+=localTime.secs(castor::utils::Timer::resetCounter);
    
    //got an exception, return false
    return false;
  }
}

//------------------------------------------------------------------------------
// DiskWriteTask::getFreeBlock
//------------------------------------------------------------------------------
MemBlock *DiskWriteTask::getFreeBlock() { 
  throw castor::exception::Exception("DiskWriteTask::getFreeBlock should mot be called");
}

//------------------------------------------------------------------------------
// DiskWriteTask::pushDataBlock
//------------------------------------------------------------------------------
void DiskWriteTask::pushDataBlock(MemBlock *mb) {
  castor::server::MutexLocker ml(&m_producerProtection);
  m_fifo.push(mb);
}

//------------------------------------------------------------------------------
// DiskWriteTask::~DiskWriteTask
//------------------------------------------------------------------------------
DiskWriteTask::~DiskWriteTask() { 
  volatile castor::server::MutexLocker ml(&m_producerProtection); 
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
        
        //disabled temporarily (see comment in MemBlock)
        //errCode=mb->errorCode();
      }
      else{
        errorMsg="Mistmatch between expected and received filed or blockid";
        errCode=SEINTERNAL;
      }
      lc.log(LOG_ERR,errorMsg);
      throw castor::exception::Exception(errorMsg);
    }
  }

//------------------------------------------------------------------------------
// getTiming
//------------------------------------------------------------------------------  
const DiskStats DiskWriteTask::getTaskStats() const{
  return m_stats;
}
//------------------------------------------------------------------------------
// logWithStat
//------------------------------------------------------------------------------  
void DiskWriteTask::logWithStat(int level,const std::string& msg,log::LogContext& lc){
  log::ScopedParamContainer params(lc);
     params.addSnprintfDouble("readWriteTime", m_stats.readWriteTime)
           .addSnprintfDouble("checksumingTime",m_stats.checksumingTime)
           .addSnprintfDouble("waitDataTime",m_stats.waitDataTime)
           .addSnprintfDouble("waitReportingTime",m_stats.waitReportingTime)
           .addSnprintfDouble("checkingErrorTime",m_stats.checkingErrorTime)
           .addSnprintfDouble("openingTime",m_stats.openingTime)
           .addSnprintfDouble("closingTime",m_stats.closingTime)
           .addSnprintfDouble("transferTime", m_stats.transferTime)
           .addSnprintfDouble("totalTime", m_stats.totalTime)
           .add("dataVolume", m_stats.dataVolume)
           .addSnprintfDouble("globalPayloadTransferSpeedMBps",
              m_stats.totalTime?1.0*m_stats.dataVolume/1000/1000/m_stats.totalTime:0)
           .addSnprintfDouble("diskPerformanceMBps",
              m_stats.transferTime?1.0*m_stats.dataVolume/1000/1000/m_stats.transferTime:0)
           .addSnprintfDouble("readWriteToTransferTimeRatio", 
              m_stats.transferTime?m_stats.readWriteTime/m_stats.transferTime:0.0)
           .add("FILEID",m_recallingFile->fileid())
           .add("path",m_recallingFile->path());
    lc.log(level,msg);
}
}}}}

