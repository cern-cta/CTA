/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
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
  
  void execute(cta::log::LogContext&  lc, cta::disk::DiskFileFactory & fileFactory,
    MigrationWatchDog & watchdog, const int threadID);
  /**
   * Return the stats of the tasks. Should be call after execute 
   * (otherwise, it is pointless)
   * @return 
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
   * log into lc all m_stats parameters with the given message at the 
   * given level
   * @param level
   * @param message
   */
  void logWithStat(int level,const std::string& msg, cta::log::LogContext&  lc) ;
  
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

}}}}

