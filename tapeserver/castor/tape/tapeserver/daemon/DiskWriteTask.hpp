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
#include "castor/tape/tapeserver/daemon/RecallMemoryManager.hpp"
#include "castor/tape/tapeserver/daemon/DataConsumer.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/DiskStats.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"

#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  class MemBlock;
/**
 * The DiskWriteFileTask is responsible to write a single file onto disk as part of a recall
 * session. Being a consumer of memory blocks, it inherits from the DataConsumer class. It also
 * inherits several methods from the DiskWriteTask (TODO: do we really need this base class?).
 */
class DiskWriteTask: public DataConsumer {
public:
  /**
   * Constructor
   * @param file: All we need to know about the file we  are recalling
   * @param mm: memory manager of the session
   */
  DiskWriteTask(cta::RetrieveJob *retrieveJob, RecallMemoryManager& mm);
  
  /**
   * Main routine: takes each memory block in the fifo and writes it to disk
   * @return true if the file has been successfully written false otherwise.
   */
  virtual bool execute(RecallReportPacker& reporter,cta::log::LogContext&  lc,
    cta::disk::DiskFileFactory & fileFactory, RecallWatchDog & watchdog,
    const int threadID);
  
  /**
   * Allows client code to return a reusable memory block. Should not been called
   * @return the pointer to the memory block that can be reused
   */
  virtual MemBlock *getFreeBlock() ;
  
  /**
   * Function used to enqueue a new memory block holding data to be written to disk
   * @param mb: corresponding memory block
   */
  virtual void pushDataBlock(MemBlock *mb);

  /**
   * Destructor (also waiting for the end of the write operation)
   */
  virtual ~DiskWriteTask();
  
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
   * This function will check the consistency of the mem block and 
   * throw exception is something goes wrong
   * @param mb The mem block to check
   * @param blockId The block id the mem blopck should be at
   * @param lc FOr logging
   */
  void checkErrors(MemBlock* mb,int blockId,cta::log::LogContext&  lc);
  
  /**
   * In case of error, it will spin on the blocks until we reach the end
   * in order to push them back into the memory manager
   */
  void releaseAllBlock();
  
  /**
   * The fifo containing the memory blocks holding data to be written to disk
   */
  cta::threading::BlockingQueue<MemBlock *> m_fifo;
  
  /** 
   * All we need to know about the file we are currently recalling
   */
  std::unique_ptr<cta::RetrieveJob> m_retrieveJob;
    
  /**
   * Reference to the Memory Manager in use
   */
  RecallMemoryManager & m_memManager;
  
  /**
   * Mutex forcing serial access to the fifo
   */
  cta::threading::Mutex m_producerProtection;
  
  /**
   * log into lc all m_stats parameters with the given message at the 
   * given level
   * @param level
   * @param message
   */
  void logWithStat(int level,const std::string& msg,cta::log::LogContext&  lc) ;
};

}}}}
