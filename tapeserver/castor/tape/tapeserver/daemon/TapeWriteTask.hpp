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
#include "castor/tape/tapeserver/daemon/MigrationMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/AtomicFlag.hpp"
#include "castor/tape/tapeserver/daemon/TapeSessionStats.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "common/Timer.hpp"
#include "scheduler/ArchiveJob.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

  class MigrationReportPacker;
  class Memblock;
  class TapeSessionStats;
/**
 * The TapeWriteFileTask is responsible to write a single file onto tape as part of a migration
 * session. Being a consumer of memory blocks, it inherits from the DataConsumer class. It also
 * inherits several methods from the TapeWriteTask (TODO: do we really need this base class?).
 */

class TapeWriteTask : public DataConsumer {
public:
  /**
   * Constructor
   * @param fSeq: file sequence number of the file to be written on tape
   * @param blockCount: number of memory blocks (TODO:?)
   * @param mm: reference to the memory manager in use
   */
  TapeWriteTask(int blockCount, cta::ArchiveJob *archiveJob,
          MigrationMemoryManager& mm,cta::threading::AtomicFlag& errorFlag);
  
  
  /**
   * @return the size of the file in byte
   */
  virtual uint64_t fileSize();
    
  /**
   * Main execution routine
   * @param session
   * @param reportPacker For reporting success of or failure of the task
   * @param lc For logging
   * @param timer
   */
  virtual void execute(castor::tape::tapeFile::WriteSession & session,
   MigrationReportPacker & reportPacker, MigrationWatchDog & watchdog,
   cta::log::LogContext&  lc, cta::utils::Timer & timer);
  
  /**
   * Used to reclaim used memory blocks
   * @return the recyclable memory block
   */
  virtual MemBlock * getFreeBlock();
  
  /**
   * This is to enqueue one memory block full of data to be written on tape
   * @param mb: the memory block in question
   */
  virtual void pushDataBlock(MemBlock *mb) ;
  
  /**
   * Destructor
   */
  virtual ~TapeWriteTask();

  /**
   * Should only be called in case of error !!
   * Just pop data block and put in back into the memory manager
   */
  void circulateMemBlocks();
  
  /**
   * Return the tasl stats. Should only be called after execute
   * @return 
   */
  const TapeSessionStats getTaskStats() const ;
private:
  /**
   * Log  all localStats' stats +  m_fileToMigrate's parameters
   * into lc with msg at the given level
   */
  void logWithStats(int level, const std::string& msg,
   cta::log::LogContext&  lc) const;
     
  /**
   *Throw an exception if  m_errorFlag is set
   */
  void hasAnotherTaskTailed() const ;
  
  /**
   * This function will check the consistency of the mem block and 
   * throw exception is something goes wrong
   * @param mb The mem block to check
   * @param memBlockId The block id the mem blopck should be at
   * @param lc FOr logging
   */
  void checkErrors(MemBlock* mb,int memBlockId,cta::log::LogContext&  lc);
    
  /**
   * Function in charge of opening the WriteFile for m_fileToMigrate
   * Throw an exception it it fails
   * @param session The session on which relies the WriteFile
   * @param lc for logging purpose
   * @return the WriteFile if everything went well
   */
  std::unique_ptr<castor::tape::tapeFile::WriteFile> openWriteFile(
  castor::tape::tapeFile::WriteSession & session,cta::log::LogContext&  lc);

  /**
   * All we need to know about the file we are migrating
   */
  std::unique_ptr<cta::ArchiveJob> m_archiveJob;
  
  /**
   * reference to the memory manager in use   
   */
  MigrationMemoryManager & m_memManager;
  
  /**
   * The fifo containing the memory blocks holding data to be written to tape
   */
  DataPipeline m_fifo;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  cta::threading::Mutex m_producerProtection;
  
  /**
   * The number of memory blocks to be used
   */
  int m_blockCount;
  
  /**
   * A shared flag among the the tasks and the task injector, set as true as soon
   * as task failed to do its job 
   */
  cta::threading::AtomicFlag& m_errorFlag;
  
  /**
   * Stats
   */
  TapeSessionStats m_taskStats;

  /**
   * LBP mode tracking
   */
  std::string m_LBPMode;
  
  /**
   * The NS archive file information
   */
  cta::common::dataStructures::ArchiveFile m_archiveFile;
  
  /**
   * The file archive result for the NS
   */
  cta::common::dataStructures::TapeFile m_tapeFile;
  
  /**
   * The remote file information
   */
  std::string m_srcURL; 

};

}}}}

