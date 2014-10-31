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
#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "castor/utils/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskReadTask::DiskReadTask(DataConsumer & destination, 
        tape::tapegateway::FileToMigrateStruct* file,
        size_t numberOfBlock,castor::server::AtomicFlag& errorFlag):
m_nextTask(destination),m_migratedFile(file),
        m_numberOfBlock(numberOfBlock),m_errorFlag(errorFlag)
{}

//------------------------------------------------------------------------------
// DiskReadTask::execute
//------------------------------------------------------------------------------
void DiskReadTask::execute(log::LogContext& lc, diskFile::DiskFileFactory & fileFactory) {
  using log::LogContext;
  using log::Param;

  castor::utils::Timer localTime;
  castor::utils::Timer totalTime(localTime);
  size_t blockId=0;
  size_t migratingFileSize=m_migratedFile->fileSize();
  MemBlock* mb=NULL;
  
  try{
    //we first check here to not even try to open the disk  if a previous task has failed
    //because the disk could the very reason why the previous one failed, 
    //so dont do the same mistake twice !
    checkMigrationFailing();
    
    std::auto_ptr<tape::diskFile::ReadFile> sourceFile(
      fileFactory.createReadFile(m_migratedFile->path()));
    log::ScopedParamContainer URLcontext(lc);
    URLcontext.add("path", m_migratedFile->path())
              .add("actualURL", sourceFile->URL());
    if(migratingFileSize != sourceFile->size()){
      throw castor::exception::Exception("Mismtach between size given by the client "
              "and the real one");
    }
    
    m_stats.openingTime+=localTime.secs(castor::utils::Timer::resetCounter);
     
    LogContext::ScopedParam sp(lc, Param("filePath",m_migratedFile->path()));
    lc.log(LOG_INFO,"Opened disk file for read");
    
    while(migratingFileSize>0){

      checkMigrationFailing();
      
      mb = m_nextTask.getFreeBlock();
      m_stats.waitFreeMemoryTime+=localTime.secs(castor::utils::Timer::resetCounter);
      
      //set metadata and read the data
      mb->m_fileid = m_migratedFile->fileid();
      mb->m_fileBlock = blockId++;
            
      migratingFileSize -= mb->m_payload.read(*sourceFile);
      m_stats.readWriteTime+=localTime.secs(castor::utils::Timer::resetCounter);

      m_stats.dataVolume += mb->m_payload.size();

      //we either read at full capacity (ie size=capacity) or if there different,
      //it should be the end => migratingFileSize should be 0. If it not, error
      if(mb->m_payload.size() != mb->m_payload.totalCapacity() && migratingFileSize>0){
        std::string erroMsg = "Error while reading a file. Did not read at full capacity but the file is not fully read";
        mb->markAsFailed(erroMsg,SEINTERNAL);
        throw castor::exception::Exception(erroMsg);
      }
      m_stats.checkingErrorTime += localTime.secs(castor::utils::Timer::resetCounter);
      
      // We are done with the block, push it to the write task
      m_nextTask.pushDataBlock(mb);
      mb=NULL;
      
    } //end of while(migratingFileSize>0)
    m_stats.filesCount++;
    m_stats.totalTime = totalTime.secs();
    // We do not have delayed open like in disk writes, so time spent 
    // transferring equals total time.
    m_stats.transferTime = m_stats.totalTime;
    logWithStat(LOG_INFO, "File successfully read from disk", lc);
  }
  catch(const castor::tape::tapeserver::daemon::ErrorFlag&){
   
    lc.log(LOG_INFO,"DiskReadTask: a previous file has failed for migration "
    "Do nothing except circulating blocks");
    circulateAllBlocks(blockId,mb);
  }
  catch(const castor::exception::Exception& e){
    // We have to pump the blocks anyway, mark them failed and then pass them back 
    // to TapeWriteTask
    // Otherwise they would be stuck into TapeWriteTask free block fifo
    // If we got here we had some job to do so there shall be at least one
    // block either at hand or available.
    // The tape write task, upon reception of the failed block will mark the 
    // session as failed, hence signalling to the remaining disk read tasks to
    // cancel as nothing more will be written to tape.
    if (!mb) {
      mb=m_nextTask.getFreeBlock();
      ++blockId;
    }
    mb->markAsFailed(e.getMessageValue(),e.code());
    m_nextTask.pushDataBlock(mb);
    mb=NULL;
    
    LogContext::ScopedParam sp(lc, Param("blockID",blockId));
    LogContext::ScopedParam sp0(lc, Param("exceptionCode",e.code()));
    LogContext::ScopedParam sp1(lc, Param("exceptionMessage", e.getMessageValue()));
    lc.log(LOG_ERR,"Exception while reading a file");
    
    //deal here the number of mem block
    circulateAllBlocks(blockId,mb);
  } //end of catch
}

//------------------------------------------------------------------------------
// DiskReadTask::circulateAllBlocks
//------------------------------------------------------------------------------
void DiskReadTask::circulateAllBlocks(size_t fromBlockId, MemBlock * mb){
  size_t blockId = fromBlockId;
  while(blockId<m_numberOfBlock) {
    if (!mb) {
      mb = m_nextTask.getFreeBlock();
      ++blockId;
    }
    mb->m_fileid = m_migratedFile->fileid();
    mb->markAsCancelled();
    m_nextTask.pushDataBlock(mb);
    mb=NULL;
  } //end of while
}

//------------------------------------------------------------------------------
// logWithStat
//------------------------------------------------------------------------------  
void DiskReadTask::logWithStat(int level,const std::string& msg,log::LogContext& lc){
  log::ScopedParamContainer params(lc);
     params.addSnprintfDouble("readWriteTime", m_stats.readWriteTime)
           .addSnprintfDouble("checksumingTime",m_stats.checksumingTime)
           .addSnprintfDouble("waitDataTime",m_stats.waitDataTime)
           .addSnprintfDouble("waitReportingTime",m_stats.waitReportingTime)
           .addSnprintfDouble("checkingErrorTime",m_stats.checkingErrorTime)
           .addSnprintfDouble("openingTime",m_stats.openingTime)
           .addSnprintfDouble("transferTime", m_stats.transferTime)
           .addSnprintfDouble("totalTime", m_stats.totalTime)
           .add("dataVolume", m_stats.dataVolume)
           .addSnprintfDouble("globalPayloadTransferSpeedMBps",
              m_stats.totalTime?1.0*m_stats.dataVolume/1000/1000/m_stats.totalTime:0)
           .addSnprintfDouble("diskPerformanceMBps",
              m_stats.transferTime?1.0*m_stats.dataVolume/1000/1000/m_stats.transferTime:0)
           .addSnprintfDouble("readWriteToTransferTimeRatio", 
              m_stats.transferTime?m_stats.readWriteTime/m_stats.transferTime:0.0)
           .add("FILEID",m_migratedFile->fileid())
           .add("path",m_migratedFile->path());
    lc.log(level,msg);
}

const DiskStats DiskReadTask::getTaskStats() const{
  return m_stats;
}

}}}}

