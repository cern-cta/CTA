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

#include "castor/tape/tapeserver/daemon/MigrationTaskInjector.hpp"

#include "castor/tape/tapeserver/daemon/ErrorFlag.hpp"

#include <memory>

using cta::log::LogContext;
using cta::log::Param;

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
MigrationTaskInjector::MigrationTaskInjector(MigrationMemoryManager& mm, DiskReadThreadPool& diskReader,
                                             TapeSingleThreadInterface<TapeWriteTask>& tapeWriter,
                                             cta::ArchiveMount& archiveMount, uint64_t maxFiles,
                                             uint64_t byteSizeThreshold, const cta::log::LogContext& lc) :
m_thread(*this),
m_memManager(mm), m_tapeWriter(tapeWriter), m_diskReader(diskReader), m_archiveMount(archiveMount), m_lc(lc),
m_maxFiles(maxFiles), m_maxBytes(byteSizeThreshold) {}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::injectBulkMigrations(std::list<std::unique_ptr<cta::ArchiveJob>>& jobs) {

  const size_t blockCapacity = m_memManager.blockCapacity();
  for (auto& job : jobs) {
    const uint64_t fileSize = job->archiveFile.fileSize;
    LogContext::ScopedParam sp[] = {LogContext::ScopedParam(m_lc, Param("fileId", job->archiveFile.archiveFileID)),
                                    LogContext::ScopedParam(m_lc, Param("fSeq", job->tapeFile.fSeq)),
                                    LogContext::ScopedParam(m_lc, Param("path", job->srcURL))};
    tape::utils::suppresUnusedVariable(sp);

    const uint64_t neededBlock = howManyBlocksNeeded(fileSize, blockCapacity);

    // We give owner ship on the archive job to the tape write task (as last user).
    // disk read task gets a bare pointer.
    // TODO: could be changed as a shared_ptr.
    auto archiveJobPtr = job.get();
    std::unique_ptr<TapeWriteTask> twt(new TapeWriteTask(neededBlock, job.release(), m_memManager, m_errorFlag));
    std::unique_ptr<DiskReadTask> drt;
    // We will skip the disk read task creation for zero-length files. Tape write task will handle the request and mark it as an
    // error.
    if (fileSize) {
      drt = std::make_unique<DiskReadTask>(*twt, archiveJobPtr, neededBlock, m_errorFlag);
    }

    m_tapeWriter.push(twt.release());
    m_lc.log(cta::log::INFO, "Created tasks for migrating a file");
    if (fileSize) {
      m_diskReader.push(drt.release());
    }
    else {
      m_lc.log(cta::log::WARNING, "Skipped disk read task creation for zero-length file.");
    }
  }
  LogContext::ScopedParam sp(m_lc, Param("numberOfFiles", jobs.size()));
  m_lc.log(cta::log::INFO, "Finished creating tasks for migrating");
}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::waitThreads() {
  m_thread.wait();
}

//------------------------------------------------------------------------------
//injectBulkMigrations
//------------------------------------------------------------------------------
void MigrationTaskInjector::startThreads() {
  m_thread.start();
}

//------------------------------------------------------------------------------
//requestInjection
//------------------------------------------------------------------------------
void MigrationTaskInjector::requestInjection(bool lastCall) {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_queue.push(Request(m_maxFiles, m_maxBytes, lastCall));
}

//------------------------------------------------------------------------------
//synchronousInjection
//------------------------------------------------------------------------------
bool MigrationTaskInjector::synchronousInjection(bool& noFilesToMigrate) {
  std::list<std::unique_ptr<cta::ArchiveJob>> jobs;
  noFilesToMigrate = false;
  try {
    //First popping of files, we multiply the number of popped files / bytes by 2 to avoid multiple mounts on Repack
    //(it is applied to ArchiveForUser and ArchiveForRepack batches)
    jobs = m_archiveMount.getNextJobBatch(2 * m_maxFiles, 2 * m_maxBytes, m_lc);
  } catch (cta::exception::Exception& ex) {
    cta::log::ScopedParamContainer scoped(m_lc);
    scoped.add("transactionId", m_archiveMount.getMountTransactionId())
      .add("byteSizeThreshold", m_maxBytes)
      .add("maxFiles", m_maxFiles)
      .add("message", ex.getMessageValue());
    m_lc.log(cta::log::ERR, "Failed to getFilesToMigrate");
    return false;
  }
  cta::log::ScopedParamContainer scoped(m_lc);
  scoped.add("byteSizeThreshold", m_maxBytes).add("maxFiles", m_maxFiles);
  if (jobs.empty()) {
    noFilesToMigrate = true;
    m_lc.log(cta::log::WARNING, "No files to migrate: empty mount");
    return false;
  }
  else {
    m_firstFseqToWrite = jobs.front()->tapeFile.fSeq;
    injectBulkMigrations(jobs);
    return true;
  }
}

//------------------------------------------------------------------------------
//finish
//------------------------------------------------------------------------------
void MigrationTaskInjector::finish() {
  cta::threading::MutexLocker ml(m_producerProtection);
  m_queue.push(Request());
}

//------------------------------------------------------------------------------
//signalEndDataMovement
//------------------------------------------------------------------------------
void MigrationTaskInjector::signalEndDataMovement() {
  //first send the end signal to the threads
  m_tapeWriter.finish();
  m_diskReader.finish();
  m_memManager.finish();
}

//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void MigrationTaskInjector::WorkerThread::run() {
  m_parent.m_lc.pushOrReplace(Param("thread", "MigrationTaskInjector"));
  m_parent.m_lc.log(cta::log::INFO, "Starting MigrationTaskInjector thread");
  try {
    while (true) {
      if (m_parent.m_errorFlag) {
        throw castor::tape::tapeserver::daemon::ErrorFlag();
      }
      Request req = m_parent.m_queue.pop();
      auto jobs = m_parent.m_archiveMount.getNextJobBatch(req.filesRequested, req.bytesRequested, m_parent.m_lc);
      uint64_t files = jobs.size();
      uint64_t bytes = 0;
      for (auto& j : jobs) bytes += j->archiveFile.fileSize;
      if (jobs.empty()) {
        if (req.lastCall) {
          m_parent.m_lc.log(cta::log::INFO, "No more file to migrate: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        }
        else {
          m_parent.m_lc.log(cta::log::INFO,
                            "In MigrationTaskInjector::WorkerThread::run(): got empty list, but not last call");
        }
      }
      else {

        // Inject the tasks
        m_parent.injectBulkMigrations(jobs);
        // Decide on continuation
        if (files < req.filesRequested / 2 && bytes < req.bytesRequested) {
          // The client starts to dribble files at a low rate. Better finish
          // the session now, so we get a clean batch on a later mount.
          cta::log::ScopedParamContainer params(m_parent.m_lc);
          params.add("filesRequested", req.filesRequested)
            .add("bytesRequested", req.bytesRequested)
            .add("filesReceived", files)
            .add("bytesReceived", bytes);
          m_parent.m_lc.log(cta::log::INFO,
                            "Got less than half the requested work to do: triggering the end of session.");
          m_parent.signalEndDataMovement();
          break;
        }
      }
    }  //end of while(1)
  } catch (const castor::tape::tapeserver::daemon::ErrorFlag&) {
    //we end up there because a task screw up somewhere
    m_parent.m_lc.log(cta::log::INFO, "In MigrationTaskInjector::WorkerThread::run(): a task failed, "
                                      "indicating finish of run");

    m_parent.signalEndDataMovement();
  } catch (const cta::exception::Exception& ex) {
    //we end up there because we could not talk to the client

    cta::log::ScopedParamContainer container(m_parent.m_lc);
    container.add("exception message", ex.getMessageValue());
    m_parent.m_lc.logBacktrace(cta::log::INFO, ex.backtrace());
    m_parent.m_lc.log(cta::log::ERR, "In MigrationTaskInjector::WorkerThread::run(): "
                                     "could not retrieve a list of file to migrate, indicating finish of run");

    m_parent.signalEndDataMovement();
  }
  //-------------
  m_parent.m_lc.log(cta::log::INFO, "Finishing MigrationTaskInjector thread");
  /* We want to finish at the first lastCall we encounter.
     * But even after sending finish() to m_diskWriter and to m_tapeReader,
     * m_diskWriter might still want some more task (the threshold could be crossed),
     * so we discard everything that might still be in the queue
     */
  bool stillReading = true;
  while (stillReading) {
    Request req = m_parent.m_queue.pop();
    if (req.end) stillReading = false;
    LogContext::ScopedParam sp(m_parent.m_lc, Param("lastCall", req.lastCall));
    m_parent.m_lc.log(cta::log::INFO, "In MigrationTaskInjector::WorkerThread::run(): popping extra request");
  }
}

uint64_t MigrationTaskInjector::firstFseqToWrite() const {
  return m_firstFseqToWrite;
}
}  //end namespace daemon
}  //end namespace tapeserver
}  //end namespace tape
}  //end namespace castor
