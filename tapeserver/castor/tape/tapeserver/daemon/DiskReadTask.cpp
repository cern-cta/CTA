/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/DiskReadTask.hpp"
#include "common/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskReadTask::DiskReadTask(DataConsumer & destination, 
        cta::ArchiveJob *archiveJob,
        size_t numberOfBlock,cta::threading::AtomicFlag& errorFlag):
m_nextTask(destination),m_archiveJob(archiveJob),
        m_numberOfBlock(numberOfBlock),m_errorFlag(errorFlag)
{
  m_archiveJobCachedInfo.remotePath = m_archiveJob->srcURL;
  m_archiveJobCachedInfo.fileId = m_archiveJob->archiveFile.archiveFileID;
}

//------------------------------------------------------------------------------
// DiskReadTask::execute
//------------------------------------------------------------------------------
void DiskReadTask::execute(cta::log::LogContext&  lc, cta::disk::DiskFileFactory & fileFactory,
    MigrationWatchDog & watchdog, const int threadID) {
  using cta::log::LogContext;
  using cta::log::Param;

  cta::utils::Timer localTime;
  cta::utils::Timer totalTime(localTime);
  size_t blockId=0;
  size_t migratingFileSize=m_archiveJob->archiveFile.fileSize;
  MemBlock* mb = nullptr;
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
    std::unique_ptr<cta::disk::ReadFile> sourceFile(
      fileFactory.createReadFile(m_archiveJob->srcURL));
    cta::log::ScopedParamContainer URLcontext(lc);
    URLcontext.add("path", m_archiveJob->srcURL)
              .add("actualURL", sourceFile->URL());
    currentErrorToCount = "Error_diskFileToReadSizeMismatch";
    if(migratingFileSize != sourceFile->size()){
      throw cta::exception::Exception("Mismatch between size given by the client "
              "and the real one");
    }
    currentErrorToCount = "";
    
    m_stats.openingTime+=localTime.secs(cta::utils::Timer::resetCounter);
     
    LogContext::ScopedParam sp(lc, Param("fileId",m_archiveJob->archiveFile.archiveFileID));
    lc.log(cta::log::INFO,"Opened disk file for read");

    watchdog.addParameter(cta::log::Param("stillOpenFileForThread"+
      std::to_string((long long)threadID), sourceFile->URL()));
    
    while(migratingFileSize>0){

      checkMigrationFailing();
      
      mb = m_nextTask.getFreeBlock();
      m_stats.waitFreeMemoryTime+=localTime.secs(cta::utils::Timer::resetCounter);
      
      //set metadata and read the data
      mb->m_fileid = m_archiveJob->archiveFile.archiveFileID;
      mb->m_fileBlock = blockId++;
      
      currentErrorToCount = "Error_diskRead";
      migratingFileSize -= mb->m_payload.read(*sourceFile);
      m_stats.readWriteTime+=localTime.secs(cta::utils::Timer::resetCounter);

      m_stats.dataVolume += mb->m_payload.size();

      //we either read at full capacity (ie size=capacity, i.e. fill up the block),
      // or if there different, it should be the end of the file=> migratingFileSize 
      // should be 0. If it not, it is an error
      currentErrorToCount = "Error_diskUnexpectedSizeWhenReading";
      if(mb->m_payload.size() != mb->m_payload.totalCapacity() && migratingFileSize>0){
        std::string erroMsg = "Error while reading a file: memory block not filled up, but the file is not fully read yet";
        // Log the error
        cta::log::ScopedParamContainer params(lc);
        params.add("bytesInBlock", mb->m_payload.size())
              .add("BlockCapacity", mb->m_payload.totalCapacity())
              .add("BytesNotYetRead", migratingFileSize);
        lc.log(cta::log::ERR, "Error while reading a file: memory block not filled up, but the file is not fully read yet");
        // Mark the block as failed
        mb->markAsFailed(erroMsg);
        // Transmit to the tape write task, which will finish the session
        m_nextTask.pushDataBlock(mb);
        // Fail the disk side.
        throw cta::exception::Exception(erroMsg);
      }
      currentErrorToCount = "";
      m_stats.checkingErrorTime += localTime.secs(cta::utils::Timer::resetCounter);
      
      // We are done with the block, push it to the write task
      m_nextTask.pushDataBlock(mb);
      mb = nullptr;
      
    } //end of while(migratingFileSize>0)
    m_stats.filesCount++;
    m_stats.totalTime = totalTime.secs();
    // We do not have delayed open like in disk writes, so time spent 
    // transferring equals total time.
    m_stats.transferTime = m_stats.totalTime;
    logWithStat(cta::log::INFO, "File successfully read from disk", lc);
  }
  catch(const castor::tape::tapeserver::daemon::ErrorFlag&){
   
    lc.log(cta::log::DEBUG,"DiskReadTask: a previous file has failed for migration "
    "Do nothing except circulating blocks");
    circulateAllBlocks(blockId,mb);
  }
  catch(const cta::exception::Exception& e){
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
    mb->markAsFailed(e.getMessageValue());
    m_nextTask.pushDataBlock(mb);
    mb = nullptr;
    
    cta::log::ScopedParamContainer spc(lc);
    spc.add("blockID",blockId)
       .add("exceptionMessage", e.getMessageValue())
       .add("fileSize",m_archiveJob->archiveFile.fileSize);
    m_archiveJob->archiveFile.checksumBlob.addFirstChecksumToLog(spc);
    lc.log(cta::log::ERR,"Exception while reading a file");
    
    //deal here the number of mem block
    circulateAllBlocks(blockId,mb);
  } //end of catch
  watchdog.deleteParameter("stillOpenFileForThread"+
    std::to_string((long long)threadID));
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
    mb->m_fileid = m_archiveJob->archiveFile.archiveFileID;
    mb->markAsCancelled();
    m_nextTask.pushDataBlock(mb);
    mb = nullptr;
  } //end of while
}

//------------------------------------------------------------------------------
// logWithStat
//------------------------------------------------------------------------------  
void DiskReadTask::logWithStat(int level,const std::string& msg,cta::log::LogContext&  lc){
  cta::log::ScopedParamContainer params(lc);
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
           .add("fileId",m_archiveJobCachedInfo.fileId)
           .add("path",m_archiveJobCachedInfo.remotePath);
    lc.log(level,msg);
}

const DiskStats DiskReadTask::getTaskStats() const{
  return m_stats;
}

}}}}

