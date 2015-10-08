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
#include "serrno.h"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskReadTask::DiskReadTask(DataConsumer & destination, 
        cta::ArchiveJob *archiveJob,
        size_t numberOfBlock,castor::server::AtomicFlag& errorFlag):
m_nextTask(destination),m_archiveJob(archiveJob),
        m_numberOfBlock(numberOfBlock),m_errorFlag(errorFlag)
{}

//------------------------------------------------------------------------------
// DiskReadTask::execute
//------------------------------------------------------------------------------
void DiskReadTask::execute(log::LogContext& lc, diskFile::DiskFileFactory & fileFactory,
    MigrationWatchDog & watchdog) {
  using log::LogContext;
  using log::Param;

  castor::utils::Timer localTime;
  castor::utils::Timer totalTime(localTime);
  size_t blockId=0;
  size_t migratingFileSize=m_archiveJob->archiveFile.size;
  MemBlock* mb=NULL;
  // This out-of-try-catch variables allows us to record the stage of the 
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::string currentErrorToCount = "";
  try{
    //we first check here to not even try to open the disk  if a previous task has failed
    //because the disk could the very reason why the previous one failed, 
    //so dont do the same mistake twice !
    checkMigrationFailing();
    currentErrorToCount = "Error_diskOpenForRead";
    std::unique_ptr<tape::diskFile::ReadFile> sourceFile(
      fileFactory.createReadFile(m_archiveJob->remotePathAndStatus.path.getRaw()));
    log::ScopedParamContainer URLcontext(lc);
    URLcontext.add("path", m_archiveJob->remotePathAndStatus.path.getRaw())
              .add("actualURL", sourceFile->URL());
    currentErrorToCount = "Error_diskFileToReadSizeMismatch";
    if(migratingFileSize != sourceFile->size()){
      throw castor::exception::Exception("Mismatch between size given by the client "
              "and the real one");
    }
    currentErrorToCount = "";
    
    m_stats.openingTime+=localTime.secs(castor::utils::Timer::resetCounter);
     
    LogContext::ScopedParam sp(lc, Param("filePath",m_archiveJob->archiveFile.path));
    lc.log(LOG_INFO,"Opened disk file for read");
    
    while(migratingFileSize>0){

      checkMigrationFailing();
      
      mb = m_nextTask.getFreeBlock();
      m_stats.waitFreeMemoryTime+=localTime.secs(castor::utils::Timer::resetCounter);
      
      //set metadata and read the data
      mb->m_fileid = m_archiveJob->archiveFile.fileId;
      mb->m_fileBlock = blockId++;
      
      currentErrorToCount = "Error_diskRead";
      migratingFileSize -= mb->m_payload.read(*sourceFile);
      m_stats.readWriteTime+=localTime.secs(castor::utils::Timer::resetCounter);

      m_stats.dataVolume += mb->m_payload.size();

      //we either read at full capacity (ie size=capacity, i.e. fill up the block),
      // or if there different, it should be the end of the file=> migratingFileSize 
      // should be 0. If it not, it is an error
      currentErrorToCount = "Error_diskUnexpectedSizeWhenReading";
      if(mb->m_payload.size() != mb->m_payload.totalCapacity() && migratingFileSize>0){
        std::string erroMsg = "Error while reading a file: memory block not filled up, but the file is not fully read yet";
        // Log the error
        log::ScopedParamContainer params(lc);
        params.add("bytesInBlock", mb->m_payload.size())
              .add("BlockCapacity", mb->m_payload.totalCapacity())
              .add("BytesNotYetRead", migratingFileSize);
        lc.log(LOG_ERR, "Error while reading a file: memory block not filled up, but the file is not fully read yet");
        // Mark the block as failed
        mb->markAsFailed(erroMsg,SEINTERNAL);
        // Transmit to the tape write task, which will finish the session
        m_nextTask.pushDataBlock(mb);
        // Fail the disk side.
        throw castor::exception::Exception(erroMsg);
      }
      currentErrorToCount = "";
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
   
    lc.log(LOG_DEBUG,"DiskReadTask: a previous file has failed for migration "
    "Do nothing except circulating blocks");
    circulateAllBlocks(blockId,mb);
  }
  catch(const castor::exception::Exception& e){
    // Send the error for counting to the watchdog
    if (currentErrorToCount.size()) {
      watchdog.addToErrorCount(currentErrorToCount);
    }
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
    mb->m_fileid = m_archiveJob->archiveFile.fileId;
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
     params.add("readWriteTime", m_stats.readWriteTime)
           .add("checksumingTime",m_stats.checksumingTime)
           .add("waitFreeMemoryTime",m_stats.waitFreeMemoryTime)
           .add("waitDataTime",m_stats.waitDataTime)
           .add("waitReportingTime",m_stats.waitReportingTime)
           .add("checkingErrorTime",m_stats.checkingErrorTime)
           .add("openingTime",m_stats.openingTime)
           .add("transferTime", m_stats.transferTime)
           .add("totalTime", m_stats.totalTime)
           .add("dataVolume", m_stats.dataVolume)
           .add("globalPayloadTransferSpeedMBps",
              m_stats.totalTime?1.0*m_stats.dataVolume/1000/1000/m_stats.totalTime:0)
           .add("diskPerformanceMBps",
              m_stats.transferTime?1.0*m_stats.dataVolume/1000/1000/m_stats.transferTime:0)
           .add("openRWCloseToTransferTimeRatio", 
              m_stats.transferTime?(m_stats.openingTime+m_stats.readWriteTime+m_stats.closingTime)/m_stats.transferTime:0.0)
           .add("FILEID",m_archiveJob->archiveFile.fileId)
           .add("path",m_archiveJob->remotePathAndStatus.path.getRaw());
    lc.log(level,msg);
}

const DiskStats DiskReadTask::getTaskStats() const{
  return m_stats;
}

}}}}

