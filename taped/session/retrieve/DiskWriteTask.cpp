/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskWriteTask.hpp"

#include "common/log/LogContext.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/utils/Timer.hpp"
#include "taped/session/pipeline/AutoReleaseBlock.hpp"
#include "taped/session/pipeline/MemBlock.hpp"
#include "taped/session/pipeline/TransferTaskTracker.hpp"
#include "telemetry/metrics/TapedMetrics.hpp"

#include <format>
#include <optional>

namespace cta::tape::daemon {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DiskWriteTask::DiskWriteTask(cta::RetrieveJob* retrieveJob, RecallMemoryManager& mm)
    : m_retrieveJob(retrieveJob),
      m_memManager(mm) {}

//------------------------------------------------------------------------------
// DiskWriteTask::execute
//------------------------------------------------------------------------------
bool DiskWriteTask::execute(RecallReportPacker& reporter,
                            cta::log::LogContext& lc,
                            cta::disk::DiskFileFactory& fileFactory,
                            TapeSessionTracker& tracker,
                            int threadID) {
  [[maybe_unused]] TransferTaskTracker transferTaskTracer(cta::semconv::attr::CtaIoDirectionValues::kWrite,
                                                          cta::semconv::attr::CtaIoMediumValues::kDisk);
  using cta::log::LogContext;
  using cta::log::Param;
  cta::utils::Timer localTime;
  cta::utils::Timer totalTime(localTime);
  cta::utils::Timer transferTime(localTime);
  cta::log::ScopedParamContainer URLcontext(lc);
  URLcontext.add("fileId", m_retrieveJob->retrieveRequest.archiveFileID)
    .add("dstURL", m_retrieveJob->retrieveRequest.dstURL)
    .add("fSeq", m_retrieveJob->selectedTapeFile().fSeq);
  m_stats.dstURL = m_retrieveJob->retrieveRequest.dstURL;
  m_stats.fileId = m_retrieveJob->retrieveRequest.archiveFileID;
  // This out-of-try-catch variables allows us to record the stage of the
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::optional<TapeSessionFailure> currentErrorToCount;
  std::optional<RecordedFailure> failure;
  bool isVerifyOnly(false);

  // Keep track of whether we have received the end-of-file marker from the tape side.
  bool eofMarkerReceived = false;

  try {
    currentErrorToCount.reset();
    // Placeholder for the disk file. We will open it only
    // after getting a first correct memory block.
    std::unique_ptr<cta::disk::WriteFile> writeFile;

    int blockId = 0;
    unsigned long checksum = Payload::zeroAdler32();
    while (true) {
      if (MemBlock* const mb = m_fifo.pop(); mb != nullptr) {
        m_stats.waitDataTime += localTime.secs(cta::utils::Timer::resetCounter);
        AutoReleaseBlock releaser(mb, m_memManager);

        if (mb->isVerifyOnly()) {
          // For verifyOnly, there is no disk file to write. Ignore the memory block and continue.
          isVerifyOnly = true;
          continue;
        } else if (mb->isCanceled()) {
          // If the tape side got canceled, we report nothing and count
          // it as a success.
          lc.log(cta::log::DEBUG, "File transfer canceled");
          return true;
        }

        // Keep the upstream receipt before the block is recycled during exception unwinding.
        if (mb->isFailed() && mb->m_fileid == m_retrieveJob->retrieveRequest.archiveFileID) {
          failure = mb->recordedFailure();
        }

        // this will throw (thus exiting the loop) if there is a mismatch between the expected file ID and block ID
        // and the actual values in the memory block, or if the block has been marked as failed.
        checkErrors(mb, blockId, lc);
        m_stats.checkingErrorTime += localTime.secs(cta::utils::Timer::resetCounter);

        if (!writeFile.get()) {
          // If we get here, this means we're at the first block of the file.
          // Since the latter hasn't been opened yet, we'll do it now.
          // Note that we don't open it before so that we don't create empty files on disk.

          lc.log(cta::log::DEBUG, "About to open disk file for writing");
          // Synchronise the counter with the open time counter.
          currentErrorToCount = TapeSessionFailure::DiskOpenForWrite;

          // Initialize the transfer time counter to measure the time spent transferring the file to disk
          transferTime = localTime;

          writeFile.reset(fileFactory.createWriteFile(m_retrieveJob->retrieveRequest.dstURL));
          URLcontext.add("actualURL", writeFile->URL());
          lc.log(cta::log::INFO, "Opened disk file for writing");

          // Record the time spent opening the file.
          m_stats.openingTime += localTime.secs(cta::utils::Timer::resetCounter);

          // Notify the session tracker that the disk file has been opened
          tracker.notifyDiskFileOpened(threadID, m_retrieveJob->retrieveRequest.archiveFileID, writeFile->URL());
        }

        // Write the data.
        currentErrorToCount = TapeSessionFailure::DiskWrite;
        m_stats.dataVolume += mb->m_payload.size();
        if (mb->m_payload.size()) {
          mb->m_payload.write(*writeFile);
        }
        m_stats.readWriteTime += localTime.secs(cta::utils::Timer::resetCounter);

        // update the (cumulative) checksum of the data block
        checksum = mb->m_payload.adler32(checksum);
        m_stats.checksumingTime += localTime.secs(cta::utils::Timer::resetCounter);
        currentErrorToCount.reset();

        blockId++;
        //end if block non nullptr
      } else if (isVerifyOnly) {
        // We're only verifying the file's checksum, so we don't expect any more data blocks.
        // The tape side has pushed a nullptr to indicate that the file is complete.
        eofMarkerReceived = true;
        break;
      } else {
        // The tape side has pushed a nullptr and we're not in verify-only mode. This means
        // that we reached the last block of the file. We can now close the disk file.
        eofMarkerReceived = true;
        //close has to be explicit, because it may throw.
        //A close is done  in WriteFile's destructor, but it may lead to some
        //silent data loss
        currentErrorToCount = TapeSessionFailure::DiskCloseAfterWrite;

        // send the close event to the disk system, which may throw an exception if the file
        // is rejected (e.g. if the disk system checks the file size, or a checksum, and finds
        // it does not match the expected value)
        writeFile->close();
        m_stats.closingTime += localTime.secs(cta::utils::Timer::resetCounter);
        m_stats.filesCount++;
        break;
      }
    }  //end of while(1)
    m_retrieveJob->transferredSize = m_stats.dataVolume;
    m_retrieveJob->transferredChecksumType = "ADLER32";
    m_retrieveJob->transferredChecksumValue = std::format("{:08x}", static_cast<uint32_t>(checksum));
    reporter.reportCompletedJob(std::move(m_retrieveJob), lc);
    m_stats.waitReportingTime += localTime.secs(cta::utils::Timer::resetCounter);
    tracker.addDiskTransferStats({.waitReportingTime = m_stats.waitReportingTime});
    m_stats.transferTime = transferTime.secs();
    m_stats.totalTime = totalTime.secs();
    logWithStat(cta::log::INFO,
                isVerifyOnly ? "File successfully verified" : "File successfully transfered to disk",
                lc);
    tracker.notifyDiskFileClosed(threadID);
    cta::telemetry::metrics::ctaTapedTransferFileCount->Add(
      1,
      {
        {cta::semconv::attr::kCtaIoDirection, cta::semconv::attr::CtaIoDirectionValues::kWrite},
        {cta::semconv::attr::kCtaIoMedium,    cta::semconv::attr::CtaIoMediumValues::kDisk    }
    });
    cta::telemetry::metrics::ctaTapedTransferFileSize->Add(
      m_stats.dataVolume,
      {
        {cta::semconv::attr::kCtaIoDirection, cta::semconv::attr::CtaIoDirectionValues::kWrite},
        {cta::semconv::attr::kCtaIoMedium,    cta::semconv::attr::CtaIoMediumValues::kDisk    }
    });
    //everything went well, return true
    return true;
  }  //end of try
  catch (const cta::exception::Exception& e) {
    /*
     *We might end up there because ;
     * -- WriteFile failed
     * -- A desynchronization between tape read and disk write
     * -- An error in tape read
     * -- An error while writing the file
     */
    cta::telemetry::metrics::ctaTapedTransferFileCount->Add(
      1,
      {
        {cta::semconv::attr::kCtaIoDirection, cta::semconv::attr::CtaIoDirectionValues::kWrite},
        {cta::semconv::attr::kCtaIoMedium,    cta::semconv::attr::CtaIoMediumValues::kDisk    },
        {cta::semconv::attr::kErrorType,      cta::semconv::attr::ErrorTypeValues::kException }
    });

    // Return the memory blocks to the pool only if we have not received the end-of-file marker
    // from the tape side. Otherwise, the memory blocks will have already been released. We
    // should not attempt to release them again, or `releaseAllMemoryBlocks()` will be stuck
    // waiting for a EOF marker that will never arrive.
    if (!eofMarkerReceived) {
      releaseAllMemoryBlocks();
    }

    // Propagate the error to the session tracker.
    if (currentErrorToCount) {
      failure = tracker.recordFailure(*currentErrorToCount);
    } else if (!failure) {
      failure = tracker.recordFailure(TapeSessionFailure::UnclassifiedFile);
    }

    m_stats.waitReportingTime += localTime.secs(cta::utils::Timer::resetCounter);
    tracker.addDiskTransferStats({.waitReportingTime = m_stats.waitReportingTime});
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::exceptionMessage, e.getMessageValue());
    logWithStat(cta::log::ERR, isVerifyOnly ? "File verification failed" : "File writing to disk failed", lc);
    lc.logBacktrace(cta::log::INFO, e.backtrace());
    reporter.reportFailedJob(std::move(m_retrieveJob), e, lc, *failure);

    tracker.notifyDiskFileClosed(threadID);

    //got an exception, return false
    return false;
  }
}

//------------------------------------------------------------------------------
// DiskWriteTask::getFreeBlock
//------------------------------------------------------------------------------
MemBlock* DiskWriteTask::getFreeBlock() {
  throw cta::exception::Exception("DiskWriteTask::getFreeBlock should mot be called");
}

//------------------------------------------------------------------------------
// DiskWriteTask::pushDataBlock
//------------------------------------------------------------------------------
void DiskWriteTask::pushDataBlock(MemBlock* mb) {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_fifo.push(mb);
}

//------------------------------------------------------------------------------
// DiskWriteTask::~DiskWriteTask
//------------------------------------------------------------------------------
DiskWriteTask::~DiskWriteTask() {
  volatile cta::threading::MutexLocker ml(m_producerProtection);
}

//------------------------------------------------------------------------------
// DiskWriteTask::releaseAllMemoryBlocks
//------------------------------------------------------------------------------
void DiskWriteTask::releaseAllMemoryBlocks() {
  while (true) {
    if (MemBlock* mb = m_fifo.pop(); mb != nullptr) {
      AutoReleaseBlock release(mb, m_memManager);
    } else {
      break;
    }
  }
}

//------------------------------------------------------------------------------
// checkErrors
//------------------------------------------------------------------------------
void DiskWriteTask::checkErrors(MemBlock* mb, uint64_t blockId, cta::log::LogContext& lc) const {
  using namespace cta::log;
  // Check that the block is consistent with the expected file ID and block ID and that it has not been marked as
  // failed. If any of these conditions are not met, log the error and throw an exception.
  if (m_retrieveJob->retrieveRequest.archiveFileID != mb->m_fileid || blockId != mb->m_fileBlock || mb->isFailed()) {
    [[maybe_unused]] LogContext::ScopedParam sp[] = {
      LogContext::ScopedParam(lc, Param("received_archiveFileID", mb->m_fileid)),
      LogContext::ScopedParam(lc, Param("expected_NSBLOCKId", blockId)),
      LogContext::ScopedParam(lc, Param("received_NSBLOCKId", mb->m_fileBlock)),
      LogContext::ScopedParam(lc, Param("failed_Status", mb->isFailed()))};
    std::string errorMsg;
    if (mb->isFailed()) {
      errorMsg = mb->errorMsg();
    } else {
      errorMsg = "Mismatch between expected and received fileid or blockid";
    }
    lc.log(cta::log::ERR, errorMsg);
    throw cta::exception::Exception(errorMsg);
  }
}

//------------------------------------------------------------------------------
// getTiming
//------------------------------------------------------------------------------
const DiskStats& DiskWriteTask::getTaskStats() const {
  return m_stats;
}

//------------------------------------------------------------------------------
// logWithStat
//------------------------------------------------------------------------------
void DiskWriteTask::logWithStat(int level, std::string_view msg, cta::log::LogContext& lc) const {
  cta::log::ScopedParamContainer params(lc);
  params.add("readWriteTime", m_stats.readWriteTime)
    .add("checksumingTime", m_stats.checksumingTime)
    .add("waitDataTime", m_stats.waitDataTime)
    .add("waitReportingTime", m_stats.waitReportingTime)
    .add("checkingErrorTime", m_stats.checkingErrorTime)
    .add("openingTime", m_stats.openingTime)
    .add("closingTime", m_stats.closingTime)
    .add("transferTime", m_stats.transferTime)
    .add("totalTime", m_stats.totalTime)
    .add("dataVolume", m_stats.dataVolume)
    .add("globalPayloadTransferSpeedMBps",
         m_stats.totalTime == 0.0 ? 0.0 : 1.0 * m_stats.dataVolume / 1000 / 1000 / m_stats.totalTime)
    .add("diskPerformanceMBps",
         m_stats.transferTime == 0.0 ? 0.0 : 1.0 * m_stats.dataVolume / 1000 / 1000 / m_stats.transferTime)
    .add("openRWCloseToTransferTimeRatio",
         m_stats.transferTime == 0.0 ?
           0.0 :
           (m_stats.openingTime + m_stats.readWriteTime + m_stats.closingTime) / m_stats.transferTime)
    .add("fileId", m_stats.fileId)
    .add("dstURL", m_stats.dstURL);
  lc.log(level, msg);
}

}  // namespace cta::tape::daemon
