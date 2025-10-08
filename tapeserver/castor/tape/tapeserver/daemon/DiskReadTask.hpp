/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "castor/tape/tapeserver/daemon/DataPipeline.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/DiskStats.hpp"
#include "castor/tape/tapeserver/daemon/ErrorFlag.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "common/threading/AtomicFlag.hpp"
#include "common/log/LogContext.hpp"
#include "disk/DiskFile.hpp"

namespace castor::tape::tapeserver::daemon {

class DiskReadTask {
public:
  /**
   * @param destination The task that will consume data block we fill up
   * @param file the file we are migrating. We acquire the ownership of the pointer
   * @param numberOfBlock number of memory block we need read the whole file
   */
  DiskReadTask(DataConsumer & destination,
          cta::ArchiveJob *archiveJob,size_t numberOfBlock,
          cta::threading::AtomicFlag& errorFlag);

  void execute(cta::log::LogContext& lc, cta::disk::DiskFileFactory& fileFactory,
    MigrationWatchDog& watchdog, int threadID);

  /**
   * Return the stats of the tasks
   *
   * Should be called after execute (otherwise, it is pointless)
   */
  const DiskStats getTaskStats() const;

private:

  /**
   * Stats to measue how long it takes to write on disk
   */
  DiskStats m_stats;

  /**
   * Throws an exception if m_errorFlag is set
   */
  void checkMigrationFailing() const {
    //if a task has signaled an error, we stop our job
    if(m_errorFlag){
      throw  castor::tape::tapeserver::daemon::ErrorFlag();
    }
  }

  /**
   * log into lc all m_stats parameters with the given message at the given level
   */
  void logWithStat(int level, std::string_view msg, cta::log::LogContext& lc) ;

  /**
   * Circulate the remaining free blocks after an error
   * @param fromBlockId the number of already processed
   * @param mb pointer to a possible already popped free block (nullptr otherwise)
   */
  void circulateAllBlocks(size_t fromBlockId, MemBlock * mb);
  /**
   * The task (a TapeWriteTask) that will handle the read blocks
   */
  DataConsumer & m_nextTask;

  /**
   * All we need to know about the file we are migrating
   */
  cta::ArchiveJob *m_archiveJob;

  /**
   * Information about the archive job we will cache as it is needed
   * after the archive job is going to be potentially deleted by the
   * writer
   */
  struct {
    std::string remotePath;
    uint64_t fileId;
  } m_archiveJobCachedInfo;

  /**
   * The number of memory block we will need to read the whole file
   */
  size_t m_numberOfBlock;

  cta::threading::AtomicFlag& m_errorFlag;
};

} // namespace castor::tape::tapeserver::daemon

