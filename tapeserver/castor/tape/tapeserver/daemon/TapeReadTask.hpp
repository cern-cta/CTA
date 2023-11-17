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

#pragma once

#include <memory>
#include <string>

#include "castor/tape/tapeserver/daemon/AutoReleaseBlock.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"
#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/file/FileReaderFactory.hpp"
#include "common/Timer.hpp"
#include "common/exception/Exception.hpp"

namespace castor::tape::tapeserver::daemon {

class TapeReadTask {
public:
  /**
   * Constructor
   * @param ftr The file being recalled. We acquire the ownership on the pointer
   * @param destination the task that will consume the memory blocks
   * @param mm The memory manager to get free block
   */
  TapeReadTask(cta::RetrieveJob *retrieveJob,
    DataConsumer & destination, RecallMemoryManager & mm):
    m_retrieveJob(retrieveJob), m_fifo(destination), m_mm(mm) {}

    /**
     * @param rs the read session holding all we need to be able to read from the tape
     * @param lc the log context for .. logging purpose
     * The actual function that will do the job.
     * The main loop is :
     * Acquire a free memory block from the memory manager , fill it, push it
     */
  void execute(const std::unique_ptr<castor::tape::tapeFile::ReadSession> &rs,
    cta::log::LogContext &lc, RecallWatchDog &watchdog,
    TapeSessionStats &stats, cta::utils::Timer &timer) {

    using cta::log::Param;

    const bool isRepack = m_retrieveJob->m_dbJob->isRepack;
    const bool isVerifyOnly = m_retrieveJob->retrieveRequest.isVerifyOnly;
    // Set the common context for all the coming logs (file info)
    cta::log::ScopedParamContainer params(lc);
    params.add("fileId", m_retrieveJob->archiveFile.archiveFileID)
          .add("BlockId", m_retrieveJob->selectedTapeFile().blockId)
          .add("fSeq", m_retrieveJob->selectedTapeFile().fSeq)
          .add("dstURL", m_retrieveJob->retrieveRequest.dstURL)
          .add("isRepack", isRepack)
          .add("isVerifyOnly", isVerifyOnly);

    // We will clock the stats for the file itself, and eventually add those
    // stats to the session's.
    TapeSessionStats localStats;
    std::string LBPMode;
    cta::utils::Timer localTime;
    cta::utils::Timer totalTime(localTime);

    // Read the file and transmit it
    bool stillReading = true;
    //for counting how many mem blocks have used and how many tape blocks
    //(because one mem block can hold several tape blocks
    uint64_t fileBlock = 0;
    size_t tapeBlock = 0;
    // This out-of-try-catch variables allows us to record the stage of the 
    // process we're in, and to count the error if it occurs.
    // We will not record errors for an empty string. This will allow us to
    // prevent counting where error happened upstream.
    std::string currentErrorToCount = "";
    MemBlock* mb = nullptr;
    try {
      currentErrorToCount = "Error_tapePositionForRead";
      auto reader = openFileReader(rs, lc);
      LBPMode = reader->getLBPMode();
      // At that point we already read the header.
      localStats.headerVolume += TapeSessionStats::headerVolumePerFile;

      lc.log(cta::log::INFO, "Successfully positioned for reading");
      localStats.positionTime += timer.secs(cta::utils::Timer::resetCounter);
      watchdog.notifyBeginNewJob(m_retrieveJob->archiveFile.archiveFileID, m_retrieveJob->selectedTapeFile().fSeq);
      localStats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
      currentErrorToCount = "Error_tapeReadData";
      auto checksum_adler32 = Payload::zeroAdler32();
      cta::checksum::ChecksumBlob tapeReadChecksum;
      while (stillReading) {
        // Get a memory block and add information to its metadata
        mb=m_mm.getFreeBlock();
        localStats.waitFreeMemoryTime += timer.secs(cta::utils::Timer::resetCounter);

        mb->m_fSeq = m_retrieveJob->selectedTapeFile().fSeq;
        mb->m_fileBlock = fileBlock++;
        mb->m_fileid = m_retrieveJob->retrieveRequest.archiveFileID;
        mb->m_tapeFileBlock = tapeBlock;
        mb->m_tapeBlockSize = reader->getBlockSize();
        try {
          // Fill up the memory block with tape block
          // append conveniently returns false when there will not be more space
          // for an extra tape block, and throws an exception if we reached the
          // end of file. append() also protects against reading too big tape blocks.
          while (mb->m_payload.append(*reader)) {
            tapeBlock++;
          }
        } catch (const cta::exception::EndOfFile&) {
          // append() signaled the end of the file.
          stillReading = false;
        }
        checksum_adler32 = mb->m_payload.adler32(checksum_adler32);
        localStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
        auto blockSize = mb->m_payload.size();
        localStats.dataVolume += blockSize;
	if(isRepack){
	  localStats.repackBytesCount += blockSize;
        } else if(isVerifyOnly) {
          localStats.verifiedBytesCount += blockSize;
          // Don't write the file to disk
          mb->markAsVerifyOnly();
	} else {
	  localStats.userBytesCount += blockSize;
	}
        // If we reached the end of the file, validate the checksum (throws an exception on bad checksum)
        if(!stillReading) {
          tapeReadChecksum.insert(cta::checksum::ADLER32, checksum_adler32);
          m_retrieveJob->archiveFile.checksumBlob.validate(tapeReadChecksum);
        }
        // Pass the block to the disk write task
        m_fifo.pushDataBlock(mb);
        mb = nullptr;
        watchdog.notify(blockSize);
        localStats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
      } //end of while(stillReading)
      // We have to signal the end of the tape read to the disk write task.
      m_fifo.pushDataBlock(nullptr);
      // Log the successful transfer
      localStats.totalTime = localTime.secs();
      // Count the trailer size
      localStats.headerVolume += TapeSessionStats::trailerVolumePerFile;
      // We now transmitted one file:
      localStats.filesCount++;
      if(isRepack){
	localStats.repackFilesCount++;
      } else if(isVerifyOnly) {
        localStats.verifiedFilesCount++;
      } else {
	localStats.userFilesCount++;
      }
      params.add("positionTime", localStats.positionTime)
            .add("readWriteTime", localStats.readWriteTime)
            .add("waitFreeMemoryTime",localStats.waitFreeMemoryTime)
            .add("waitReportingTime",localStats.waitReportingTime)
            .add("transferTime",localStats.transferTime())
            .add("totalTime", localStats.totalTime)
            .add("dataVolume",localStats.dataVolume)
            .add("headerVolume",localStats.headerVolume)
            .add("driveTransferSpeedMBps",
                    localStats.totalTime?(1.0*localStats.dataVolume+1.0*localStats.headerVolume)
                     /1000/1000/localStats.totalTime:0)
            .add("payloadTransferSpeedMBps",
                     localStats.totalTime?1.0*localStats.dataVolume/1000/1000/localStats.totalTime:0)
            .add("LBPMode", LBPMode)
	    .add("repackFilesCount",localStats.repackFilesCount)
	    .add("repackBytesCount",localStats.repackBytesCount)
	    .add("userFilesCount",localStats.userFilesCount)
	    .add("userBytesCount",localStats.userBytesCount)
	    .add("verifiedFilesCount",localStats.verifiedFilesCount)
	    .add("verifiedBytesCount",localStats.verifiedBytesCount)
            .add("checksumType", "ADLER32")
            .add("checksumValue", cta::checksum::ChecksumBlob::ByteArrayToHex(tapeReadChecksum.at(cta::checksum::ADLER32)));
      lc.log(cta::log::INFO, "File successfully read from tape");
      // Add the local counts to the session's
      stats.add(localStats);
    } //end of try
    catch (const cta::exception::Exception & ex) {
      // We end up here because:
      //-- openReadFile brought us here (can't position to the file)
      //-- m_payload.append brought us here (error while reading the file)
      //-- checksum validation failed (after reading the last block from tape)
      // Record the error in the watchdog
      if (currentErrorToCount.size()) {
        watchdog.addToErrorCount(currentErrorToCount);
      }
      // This is an error case. Log and signal to the disk write task
      { 
        cta::log::LogContext::ScopedParam sp0(lc, Param("fileBlock", fileBlock));
        cta::log::LogContext::ScopedParam sp1(lc, Param("ErrorMessage", ex.getMessageValue()));
        lc.log(cta::log::ERR, "Error reading a file in TapeReadFileTask");
      }
      {
        cta::log::LogContext lc2(lc.logger());
        lc2.logBacktrace(cta::log::INFO, ex.backtrace());
      }

      // mb might or might not be allocated at this point, but
      // reportErrorToDiskTask will deal with the allocation if required.
      reportErrorToDiskTask(ex.getMessageValue(), mb);
    } //end of catch
    watchdog.fileFinished();
  }
  /**
   * Get a valid block and ask to cancel the disk write task
   */
  void reportCancellationToDiskTask(){
    MemBlock* mb =m_mm.getFreeBlock();
    mb->m_fSeq = m_retrieveJob->selectedTapeFile().fSeq;
    mb->m_fileid = m_retrieveJob->retrieveRequest.archiveFileID;
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
  void reportErrorToDiskTask(const std::string& msg, MemBlock* mb = nullptr) {
    // If we are not provided with a block, allocate it and
    // fill it up
    if (!mb) {
      mb = m_mm.getFreeBlock();
      mb->m_fSeq = m_retrieveJob->selectedTapeFile().fSeq;
      mb->m_fileid = m_retrieveJob->retrieveRequest.archiveFileID;
    }
    // mark the block failed and push it (plus signal the end)
    mb->markAsFailed(msg);
    m_fifo.pushDataBlock(mb);
    m_fifo.pushDataBlock(nullptr);
  }

  /**
   * Open the file on the tape. In case of failure, log and throw
   * Copying the unique_ptr on the calling point will give us the ownership of the
   * object.
   * @return if successful, return an unique_ptr on the FileReader we want
   */
  std::unique_ptr<castor::tape::tapeFile::FileReader> openFileReader(
    const std::unique_ptr<castor::tape::tapeFile::ReadSession> &session,
    cta::log::LogContext &lc) {
    using cta::log::Param;
    typedef cta::log::LogContext::ScopedParam ScopedParam;

    std::unique_ptr<castor::tape::tapeFile::FileReader> reader;
    try {
      reader = castor::tape::tapeFile::FileReaderFactory::create(session, *m_retrieveJob);
      lc.log(cta::log::DEBUG, "Successfully opened the tape file");
    } catch (cta::exception::Exception & ex) {
      // Log the error
      ScopedParam sp0(lc, Param("ErrorMessage", ex.getMessageValue()));
      lc.log(cta::log::ERR, "Failed to open tape file for reading");
      throw;
    }
    return reader;
  }

  /**
   * All we need to know about the file we are recalling
   */
  cta::RetrieveJob *m_retrieveJob;

  /**
   * The task (seen as a Y) that will consume all the blocks we read
   */
  DataConsumer & m_fifo;

  /**
   *  The MemoryManager from whom we get free memory blocks
   */
  RecallMemoryManager & m_mm;
};

} // namespace castor::tape::tapeserver::daemon
