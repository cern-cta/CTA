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

#pragma once


#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"
#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/AutoReleaseBlock.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "castor/tape/utils/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  /**
   * This class is in charge of 
   * 
   */
class TapeReadTask {
public:
  /**
   * COnstructor
   * @param ftr The file being recalled. We acquire the ownership on the pointer
   * @param destination the task that will consume the memory blocks
   * @param mm The memory manager to get free block
   */
  TapeReadTask(castor::tape::tapegateway::FileToRecallStruct * ftr,
    DataConsumer & destination, RecallMemoryManager & mm): 
    m_fileToRecall(ftr), m_fifo(destination), m_mm(mm) {}
    
    /**
     * @param rs the read session holding all we need to be able to read from the tape
     * @param lc the log context for .. logging purpose
     * The actual function that will do the job.
     * The main loop is :
     * Acquire a free memory block from the memory manager , fill it, push it 
     */
  void execute(castor::tape::tapeFile::ReadSession & rs,
    castor::log::LogContext & lc,TaskWatchDog<detail::Recall>& watchdog,
    TapeSessionStats & stats, utils::Timer & timer) {

    using castor::log::Param;
    typedef castor::log::LogContext::ScopedParam ScopedParam;
    
    // Set the common context for all the coming logs (file info)
    ScopedParam sp0(lc, Param("NSHOSTNAME", m_fileToRecall->nshost()));
    ScopedParam sp1(lc, Param("NSFILEID", m_fileToRecall->fileid()));
    ScopedParam sp2(lc, Param("BlockId", castor::tape::tapeFile::BlockId::extract(*m_fileToRecall)));
    ScopedParam sp3(lc, Param("fSeq", m_fileToRecall->fseq()));
    ScopedParam sp4(lc, Param("fileTransactionId", m_fileToRecall->fileTransactionId()));
    
    // We will clock the stats for the file itself, and eventually add those
    // stats to the session's.
    TapeSessionStats localStats;
    utils::Timer localTime;

    // Read the file and transmit it
    bool stillReading = true;
    //for counting how many mem blocks have used and how many tape blocks
    //(because one mem block can hold several tape blocks
    int fileBlock = 0;
    int tapeBlock = 0;
    
    MemBlock* mb=NULL;
    try {
      std::auto_ptr<castor::tape::tapeFile::ReadFile> rf(openReadFile(rs,lc));
      localStats.positionTime += timer.secs(utils::Timer::resetCounter);
      watchdog.notifyBeginNewJob(*m_fileToRecall);
      localStats.waitReportingTime += timer.secs(utils::Timer::resetCounter);
      while (stillReading) {
        // Get a memory block and add information to its metadata
        mb=m_mm.getFreeBlock();
        localStats.waitFreeMemoryTime += timer.secs(utils::Timer::resetCounter);
        
        mb->m_fSeq = m_fileToRecall->fseq();
        mb->m_fileBlock = fileBlock++;
        mb->m_fileid = m_fileToRecall->fileid();
        mb->m_tapeFileBlock = tapeBlock;
        mb->m_tapeBlockSize = rf->getBlockSize();
        try {
          // Fill up the memory block with tape block
          // append conveniently returns false when there will not be more space
          // for an extra tape block, and throws an exception if we reached the
          // end of file. append() also protects against reading too big tape blocks.
          while (mb->m_payload.append(*rf)) {
            tapeBlock++;
          }
        } catch (const castor::exception::EndOfFile&) {
          // append() signaled the end of the file.
          stillReading = false;
        }
        localStats.transferTime += timer.secs(utils::Timer::resetCounter);
        localStats.dataVolume += mb->m_payload.size();
        // Pass the block to the disk write task
        m_fifo.pushDataBlock(mb);
        mb=NULL;
        watchdog.notify();
        localStats.waitReportingTime += timer.secs(utils::Timer::resetCounter);
      } //end of while(stillReading)
      //  we have to signal the end of the tape read to the disk write task.
      m_fifo.pushDataBlock(NULL);
      // Log the successful transfer
      double fileTime = localTime.secs();
      log::ScopedParamContainer params(lc);
      params.add("positionTime", localStats.positionTime)
            .add("transferTime", localStats.transferTime)
            .add("waitFreeMemoryTime",localStats.waitFreeMemoryTime)
            .add("waitReportingTime",localStats.waitReportingTime)
            .add("dataVolume",localStats.dataVolume)
            .add("totalTime", fileTime)
            .add("driveTransferSpeedMiB/s",
                    (localStats.dataVolume+localStats.headerVolume)
                     /1024/1024
                     /localStats.transferTime)
            .add("payloadTransferSpeedMB/s",
                     1.0*localStats.dataVolume/1024/1024/fileTime)
            .add("fileid",m_fileToRecall->fileid())
            .add("fseq",m_fileToRecall->fseq())
            .add("fileTransactionId",m_fileToRecall->fileTransactionId());
      lc.log(LOG_INFO, "File successfully read from drive");
      // Add the local counts to the session's
      stats.add(localStats);
    } //end of try
    catch (const castor::exception::Exception & ex) {
      //we end up there because :
      //-- openReadFile brought us here (cant put the tape into position)
      //-- m_payload.append brought us here (error while reading the file)
      // This is an error case. Log and signal to the disk write task
      { 
        castor::log::LogContext::ScopedParam sp0(lc, Param("fileBlock", fileBlock));
        castor::log::LogContext::ScopedParam sp1(lc, Param("ErrorMessage", ex.getMessageValue()));
        castor::log::LogContext::ScopedParam sp2(lc, Param("ErrorCode", ex.code()));
        lc.log(LOG_ERR, "Error reading a file block in TapeReadFileTask (backtrace follows)");
      }
      {
        castor::log::LogContext lc2(lc.logger());
        lc2.logBacktrace(LOG_ERR, ex.backtrace());
      }
      
      // mb might or might not be allocated at this point, but 
      // reportErrorToDiskTask will deal with the allocation if required.
      reportErrorToDiskTask(ex.getMessageValue(),ex.code(),mb);
    } //end of catch
    watchdog.fileFinished();
  }
  /**
   * Get a valid block and ask to cancel the disk write task
   */
  void reportCancellationToDiskTask(){
    MemBlock* mb =m_mm.getFreeBlock();
    mb->m_fSeq = m_fileToRecall->fseq();
    mb->m_fileid = m_fileToRecall->fileid();
    //mark the block cancelled and push it (plus signal the end)
     mb->markAsCancelled();
     m_fifo.pushDataBlock(mb);
  }
private:
  /**
   * Do the actual report to the disk write task
   * @param errorMsg The error message we will give to the client
   * @param mb The mem block we will use
   */
  void reportErrorToDiskTask(const std::string& msg,int code,MemBlock* mb = NULL){
    //If we are not provided with a block, allocate it and
    // fill it up
    if (!mb) {
      mb=m_mm.getFreeBlock();
      mb->m_fSeq = m_fileToRecall->fseq();
      mb->m_fileid = m_fileToRecall->fileid();
    }
    //mark the block failed and push it (plus signal the end)
     mb->markAsFailed(msg,code);
     m_fifo.pushDataBlock(mb);
     m_fifo.pushDataBlock(NULL);
   }
  /** 
   * Open the file on the tape. In case of failure, log and throw
   * Copying the auto_ptr on the calling point will give us the ownership of the 
   * object.
   * @return if successful, return an auto_ptr on the ReadFile we want
   */
  std::auto_ptr<castor::tape::tapeFile::ReadFile> openReadFile(
  castor::tape::tapeFile::ReadSession & rs, castor::log::LogContext & lc){

    using castor::log::Param;
    typedef castor::log::LogContext::ScopedParam ScopedParam;

    std::auto_ptr<castor::tape::tapeFile::ReadFile> rf;
    try {
      rf.reset(new castor::tape::tapeFile::ReadFile(&rs, *m_fileToRecall));
      lc.log(LOG_DEBUG, "Successfully opened the tape file");
    } catch (castor::exception::Exception & ex) {
      // Log the error
      ScopedParam sp0(lc, Param("ErrorMessage", ex.getMessageValue()));
      ScopedParam sp1(lc, Param("ErrorCode", ex.code()));
      lc.log(LOG_ERR, "Failed to open tape file for reading");
      throw;
    }
    return rf;
  }
  
  /**
   * All we need to know about the file we are recalling
   */
  std::auto_ptr<castor::tape::tapegateway::FileToRecallStruct> m_fileToRecall;
  
  /**
   * The task (seen as a Y) that will consume all the blocks we read
   */
  DataConsumer & m_fifo;
  
  /**
   *  The MemoryManager from whom we get free memory blocks 
   */
  RecallMemoryManager & m_mm;

};
}
}
}
}
 
