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

#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"

#include "castor/tape/tapeserver/daemon/AutoReleaseBlock.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"
#include "castor/tape/tapeserver/daemon/ErrorFlag.hpp"
#include "castor/tape/tapeserver/daemon/MemBlock.hpp"
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "castor/tape/tapeserver/utils/suppressUnusedVariable.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"

#include <memory>
#include <string>

namespace castor::tape::tapeserver::daemon {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
TapeWriteTask::TapeWriteTask(uint64_t blockCount, cta::ArchiveJob* archiveJob, MigrationMemoryManager& mm,
                             cta::threading::AtomicFlag& errorFlag) :
m_archiveJob(archiveJob),
m_memManager(mm), m_fifo(blockCount), m_blockCount(blockCount), m_errorFlag(errorFlag),
m_archiveFile(m_archiveJob->archiveFile), m_tapeFile(m_archiveJob->tapeFile), m_srcURL(m_archiveJob->srcURL) {
  // Register its fifo to the memory manager as a client in order to get mem block
  // This should not be done in the case of a zero length file.
  if (archiveJob->archiveFile.fileSize) {
    mm.addClient(&m_fifo);
  }
}

//------------------------------------------------------------------------------
// fileSize
//------------------------------------------------------------------------------
uint64_t TapeWriteTask::fileSize() {
  return m_archiveFile.fileSize;
}

//------------------------------------------------------------------------------
// execute
//------------------------------------------------------------------------------
void TapeWriteTask::execute(const std::unique_ptr<castor::tape::tapeFile::WriteSession>& session,
                            MigrationReportPacker& reportPacker, MigrationWatchDog& watchdog, cta::log::LogContext& lc,
                            cta::utils::Timer& timer) {
  using cta::log::LogContext;
  using cta::log::Param;
  using cta::log::ScopedParamContainer;
  // Add to our logs the informations on the file
  ScopedParamContainer params(lc);
  params.add("fileId", m_archiveJob->archiveFile.archiveFileID)
    .add("fileSize", m_archiveJob->archiveFile.fileSize)
    .add("fSeq", m_archiveJob->tapeFile.fSeq)
    .add("diskURL", m_archiveJob->srcURL);

  // We will clock the stats for the file itself, and eventually add those
  // stats to the session's.
  cta::utils::Timer localTime;
  unsigned long ckSum = Payload::zeroAdler32();
  uint64_t memBlockId = 0;

  // This out-of-try-catch variables allows us to record the stage of the
  // process we're in, and to count the error if it occurs.
  // We will not record errors for an empty string. This will allow us to
  // prevent counting where error happened upstream.
  std::string currentErrorToCount = "Error_tapeFSeqOutOfSequenceForWrite";
  session->validateNextFSeq(m_archiveJob->tapeFile.fSeq);
  try {
    // Try to open the session
    currentErrorToCount = "Error_tapeWriteHeader";
    watchdog.notifyBeginNewJob(m_archiveJob->archiveFile.archiveFileID, m_archiveJob->tapeFile.fSeq);
    std::unique_ptr<castor::tape::tapeFile::FileWriter> output(openFileWriter(session, lc));
    m_LBPMode = output->getLBPMode();
    m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
    m_taskStats.headerVolume += TapeSessionStats::headerVolumePerFile;
    // We are not error sources here until we actually write.
    currentErrorToCount = "";
    bool firstBlock = true;
    while (!m_fifo.finished()) {
      MemBlock* const mb = m_fifo.popDataBlock();
      m_taskStats.waitDataTime += timer.secs(cta::utils::Timer::resetCounter);
      AutoReleaseBlock<MigrationMemoryManager> releaser(mb, m_memManager);

      // Special treatment for 1st block. If disk failed to provide anything, we can skip the file
      // by leaving a placeholder on the tape (at minimal tape space cost), so we can continue
      // the tape session (and save a tape mount!).
      if (firstBlock && mb->isFailed()) {
        currentErrorToCount = "Error_tapeWriteData";
        const char blank[] = "This file intentionally left blank: leaving placeholder after failing to read from disk.";
        output->write(blank, sizeof(blank));
        m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
        watchdog.notify(sizeof(blank));
        currentErrorToCount = "Error_tapeWriteTrailer";
        output->close();
        currentErrorToCount = "";
        // Possibly failing writes are finished. We can continue this in catch for skip. outside of the loop.
        throw Skip(mb->errorMsg());
      }
      firstBlock = false;

      // Will throw (thus exiting the loop) if something is wrong
      checkErrors(mb, memBlockId, lc);

      ckSum = mb->m_payload.adler32(ckSum);
      m_taskStats.checksumingTime += timer.secs(cta::utils::Timer::resetCounter);
      currentErrorToCount = "Error_tapeWriteData";
      mb->m_payload.write(*output);
      currentErrorToCount = "";

      m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
      m_taskStats.dataVolume += mb->m_payload.size();
      watchdog.notify(mb->m_payload.size());
      ++memBlockId;
    }

    // If, after the FIFO is finished, we are still in the first block, we are in the presence of a 0-length file.
    // This also requires a placeholder.
    if (firstBlock) {
      currentErrorToCount = "Error_tapeWriteData";
      const char blank[] = "This file intentionally left blank: zero-length file cannot be recorded to tape.";
      output->write(blank, sizeof(blank));
      m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
      watchdog.notify(sizeof(blank));
      currentErrorToCount = "Error_tapeWriteTrailer";
      output->close();
      currentErrorToCount = "";
      // Possibly failing writes are finished. We can continue this in catch for skip. outside of the loop.
      throw Skip("In TapeWriteTask::execute(): inserted a placeholder for zero length file.");
    }

    // Finish the writing of the file on tape
    // ut the trailer
    currentErrorToCount = "Error_tapeWriteTrailer";
    output->close();
    currentErrorToCount = "";
    m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
    m_taskStats.headerVolume += TapeSessionStats::trailerVolumePerFile;
    m_taskStats.filesCount++;
    // Record the fSeq in the tape session
    session->reportWrittenFSeq(m_archiveJob->tapeFile.fSeq);
    m_archiveJob->tapeFile.checksumBlob.insert(cta::checksum::ADLER32, ckSum);
    m_archiveJob->tapeFile.fileSize = m_taskStats.dataVolume;
    m_archiveJob->tapeFile.blockId = output->getBlockId();

    reportPacker.reportCompletedJob(std::move(m_archiveJob), lc);
    m_taskStats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
    m_taskStats.totalTime = localTime.secs();
    // Log the successful transfer
    logWithStats(cta::log::INFO, "File successfully transmitted to drive", lc);
  } catch (const castor::tape::tapeserver::daemon::ErrorFlag&) {
    // We end up there because another task has failed
    // so we just log, circulate blocks and don't even send a report
    lc.log(cta::log::DEBUG, "TapeWriteTask: a previous file has failed for migration "
                            "Do nothing except circulating blocks");
    circulateMemBlocks();

    // We throw again because we want TWST to stop all tasks from execution
    // and go into a degraded mode operation.
    throw;
  } catch (const Skip& s) {
    // We failed to read anything from the file. We can get rid of any block from the queue to
    // recycle them, and pass the report to the report packer. After than, we can carry on with
    // the write session->
    circulateMemBlocks();
    watchdog.addToErrorCount("Info_fileSkipped");
    m_taskStats.readWriteTime += timer.secs(cta::utils::Timer::resetCounter);
    m_taskStats.headerVolume += TapeSessionStats::trailerVolumePerFile;
    m_taskStats.filesCount++;
    // Record the fSeq in the tape session
    session->reportWrittenFSeq(m_archiveJob->tapeFile.fSeq);
    reportPacker.reportSkippedJob(std::move(m_archiveJob), s, lc);
    m_taskStats.waitReportingTime += timer.secs(cta::utils::Timer::resetCounter);
    m_taskStats.totalTime = localTime.secs();
    // Log the successful transfer
    logWithStats(cta::log::INFO, "Left placeholder on tape after skipping unreadable file.", lc);
  } catch (const RecoverableMigrationErrorException& e) {
    // The disk reading failed due to a size mismatch or wrong checksum
    // just want to report a failed job and proceed with the mount
    if (!currentErrorToCount.empty()) {
      watchdog.addToErrorCount(currentErrorToCount);
    }
    // Log and circulate blocks
    LogContext::ScopedParam sp(lc, Param("exceptionCode", cta::log::ERR));
    LogContext::ScopedParam sp1(lc, Param("exceptionMessage", e.getMessageValue()));
    lc.log(cta::log::ERR, "An error occurred for this file, but migration will proceed as error is recoverable");
    circulateMemBlocks();
    // Record the fSeq in the tape session
    session->reportWrittenFSeq(m_archiveJob->tapeFile.fSeq);
    reportPacker.reportFailedJob(std::move(m_archiveJob), e, lc);
    lc.log(cta::log::INFO, "Left placeholder on tape after skipping unreadable file.");
    return;

  } catch (const cta::exception::Exception& e) {
    // We can end up there because
    // We failed to open the FileWriter
    // We received a bad block or a block written failed
    // close failed

    // First set the error flag: we can't proceed any further with writes.
    m_errorFlag.set();

    // If we reached the end of tape, this is not an error (ENOSPC)
    try {
      // If it's not the error we're looking for, we will go about our business
      // in the catch section. dynamic cast will throw, and we'll do ourselves
      // if the error code is not the one we want.
      const auto& en = dynamic_cast<const cta::exception::Errnum&>(e);
      if (en.errorNumber() != ENOSPC) {
        throw;
      }
      // This is indeed the end of the tape. Not an error.
      watchdog.setErrorCount("Info_tapeFilledUp", 1);
      reportPacker.reportTapeFull(lc);
    } catch (...) {
      // The error is not an ENOSPC, so it is, indeed, an error.
      // If we got here with a new error, currentErrorToCount will be non-empty,
      // and we will pass the error name to the watchdog.
      if (!currentErrorToCount.empty()) {
        watchdog.addToErrorCount(currentErrorToCount);
      }
    }

    // Log and circulate blocks
    // We want to report internal error most of the time to avoid wrong
    // interpretation down the chain. Nevertheless, if the exception
    // if of type Errnum AND the errorCode is ENOSPC, we will propagate it.
    // This is how we communicate the fact that a tape is full to the client.
    // We also change the log level to INFO for the case of end of tape.
    int errorLevel = cta::log::ERR;
    bool doReportJobError = true;
    try {
      const auto& errnum = dynamic_cast<const cta::exception::Errnum&>(e);
      if (ENOSPC == errnum.errorNumber()) {
        errorLevel = cta::log::INFO;
        doReportJobError = false;
      }
    } catch (...) {}
    LogContext::ScopedParam sp1(lc, Param("exceptionMessage", e.getMessageValue()));
    lc.log(errorLevel, "An error occurred for this file. End of migrations.");
    circulateMemBlocks();
    if (doReportJobError) reportPacker.reportFailedJob(std::move(m_archiveJob), e, lc);

    // We throw again because we want TWST to stop all tasks from execution
    // and go into a degraded mode operation.
    throw;
  }
  watchdog.fileFinished();
}

//------------------------------------------------------------------------------
// getFreeBlock
//------------------------------------------------------------------------------
MemBlock* TapeWriteTask::getFreeBlock() {
  return m_fifo.getFreeBlock();
}

//------------------------------------------------------------------------------
// checkErrors
//------------------------------------------------------------------------------
void TapeWriteTask::checkErrors(MemBlock* mb, uint64_t memBlockId, cta::log::LogContext& lc) {
  using namespace cta::log;
  if (m_archiveJob->archiveFile.archiveFileID != mb->m_fileid || memBlockId != mb->m_fileBlock || mb->isFailed()
      || mb->isCanceled()) {
    LogContext::ScopedParam sp[] = {LogContext::ScopedParam(lc, Param("received_archiveFileID", mb->m_fileid)),
                                    LogContext::ScopedParam(lc, Param("expected_NSBLOCKId", memBlockId)),
                                    LogContext::ScopedParam(lc, Param("received_NSBLOCKId", mb->m_fileBlock)),
                                    LogContext::ScopedParam(lc, Param("failed_Status", mb->isFailed()))};
    tape::utils::suppresUnusedVariable(sp);
    std::string errorMsg;
    if (mb->isFailed()) {
      //blocks are marked as failed by the DiskReadTask due to a size mismatch
      //or wrong checksums
      //both errors should just result in skipping the migration of the file
      //so we use a different exception to distinguish this case
      errorMsg = mb->errorMsg();
      m_errorFlag.set();
      LogContext::ScopedParam sp1(lc, Param("errorMessage", errorMsg));
      lc.log(cta::log::ERR, "Error while reading a file");
      throw RecoverableMigrationErrorException(errorMsg);
    }
    else if (mb->isCanceled()) {
      errorMsg = "Received a block marked as cancelled";
    }
    else {
      errorMsg = "Mismatch between expected and received file id or blockid";
    }
    // Set the error flag for the session (in case of mismatch)
    m_errorFlag.set();
    lc.log(cta::log::ERR, errorMsg);
    throw cta::exception::Exception(errorMsg);
  }
}

//------------------------------------------------------------------------------
// pushDataBlock
//------------------------------------------------------------------------------
void TapeWriteTask::pushDataBlock(MemBlock* mb) {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_fifo.pushDataBlock(mb);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
TapeWriteTask::~TapeWriteTask() {
  cta::threading::MutexLocker ml(m_producerProtection);
}

//------------------------------------------------------------------------------
// openFileWriter
//------------------------------------------------------------------------------
std::unique_ptr<tapeFile::FileWriter>
  TapeWriteTask::openFileWriter(const std::unique_ptr<tape::tapeFile::WriteSession>& session,
                                cta::log::LogContext& lc) {
  std::unique_ptr<tape::tapeFile::FileWriter> output;
  try {
    const uint64_t tapeBlockSize = 256 * 1024;
    output = std::make_unique<tape::tapeFile::FileWriter>(session, *m_archiveJob, tapeBlockSize);
    lc.log(cta::log::DEBUG, "Successfully opened the tape file for writing");
  } catch (const cta::exception::Exception& ex) {
    cta::log::LogContext::ScopedParam sp(lc, cta::log::Param("exceptionMessage", ex.getMessageValue()));
    lc.log(cta::log::ERR, "Failed to open tape file for writing");
    throw;
  }
  return output;
}

//------------------------------------------------------------------------------
// circulateMemBlocks
//------------------------------------------------------------------------------
void TapeWriteTask::circulateMemBlocks() {
  while (!m_fifo.finished()) {
    m_memManager.releaseBlock(m_fifo.popDataBlock());
    // watchdog.notify();
  }
}

void TapeWriteTask::logWithStats(int level, const std::string& msg, cta::log::LogContext& lc) const {
  cta::log::ScopedParamContainer params(lc);
  params.add("readWriteTime", m_taskStats.readWriteTime)
    .add("checksumingTime", m_taskStats.checksumingTime)
    .add("waitDataTime", m_taskStats.waitDataTime)
    .add("waitReportingTime", m_taskStats.waitReportingTime)
    .add("transferTime", m_taskStats.transferTime())
    .add("totalTime", m_taskStats.totalTime)
    .add("dataVolume", m_taskStats.dataVolume)
    .add("headerVolume", m_taskStats.headerVolume)
    .add("driveTransferSpeedMBps", m_taskStats.totalTime ? 1.0 * (m_taskStats.dataVolume + m_taskStats.headerVolume)
                                                             / 1000 / 1000 / m_taskStats.totalTime :
                                                           0.0)
    .add("payloadTransferSpeedMBps",
         m_taskStats.totalTime ? 1.0 * m_taskStats.dataVolume / 1000 / 1000 / m_taskStats.totalTime : 0.0)
    .add("fileSize", m_archiveFile.fileSize)
    .add("fileId", m_archiveFile.archiveFileID)
    .add("fSeq", m_tapeFile.fSeq)
    .add("reconciliationTime", m_archiveFile.reconciliationTime)
    .add("LBPMode", m_LBPMode);

  lc.log(level, msg);
}

//------------------------------------------------------------------------------
//   getTaskStats
//------------------------------------------------------------------------------
const TapeSessionStats TapeWriteTask::getTaskStats() const {
  return m_taskStats;
}

//------------------------------------------------------------------------------
//   getArchiveJob
//------------------------------------------------------------------------------
cta::ArchiveJob& TapeWriteTask::getArchiveJob() const {
  return *m_archiveJob;
}

} // namespace castor::tape::tapeserver::daemon
