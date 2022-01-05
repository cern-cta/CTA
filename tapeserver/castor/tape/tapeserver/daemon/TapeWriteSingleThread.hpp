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

#include "common/processCap/ProcessCap.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "common/Timer.hpp"

#include <iostream>
#include <stdio.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {
// forward definition
class MigrationTaskInjector;
  
class TapeWriteSingleThread : public TapeSingleThreadInterface<TapeWriteTask> {
public:
  /**
   * Constructor
   * @param drive an interface for manipulating the drive in order 
   * to write on the tape 
   * @param vid the volume ID of the tape on which we are going to write
   * @param lc 
   * @param repPacker the object that will send reports to the client
   * @param filesBeforeFlush  how many file written before flushing on tape
   * @param bytesBeforeFlush how many bytes written before flushing on tape
   * @param lastFseq the last fSeq 
   * @param tapeLoadTimeout the timeout after which we consider the tape mount to be failed
   */
  TapeWriteSingleThread(
    castor::tape::tapeserver::drive::DriveInterface & drive, 
    cta::mediachanger::MediaChangerFacade &mc,
    TapeServerReporter & tsr,
    MigrationWatchDog & mwd,
    const VolumeInfo& volInfo,
    cta::log::LogContext & lc,
    MigrationReportPacker & repPacker,
    cta::server::ProcessCap &capUtils,
    uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush, const bool useLbp,
    const bool useEncryption,
    const std::string & externalEncryptionKeyScript,
    const cta::ArchiveMount & archiveMount,
    const uint64_t tapeLoadTimeout);
  
  /**
   * 
   * @param lastFseq
   */
  void setlastFseq(uint64_t lastFseq);
  
  /**
   * Sets up the pointer to the task injector. This cannot be done at
   * construction time as both task injector and tape write single thread refer to 
   * each other. This function should be called before starting the threads.
   * This is used for signalling problems during mounting. After that, each 
   * tape write task does the signalling itself, either on tape problem, or
   * when receiving an error from the disk tasks via memory blocks.
   * @param injector pointer to the task injector
   */
  void setTaskInjector(MigrationTaskInjector* injector){
      m_injector = injector;
  }
private:  
  
  /**
   * Returns the string representation of the specified mount type
   */
  const char *mountTypeToString(const cta::common::dataStructures::MountType mountType) const
    throw();
  
    class TapeCleaning{
    TapeWriteSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    cta::utils::Timer & m_timer;
  public:
    TapeCleaning(TapeWriteSingleThread& parent, cta::utils::Timer & timer):
      m_this(parent), m_timer(timer) {}
    ~TapeCleaning();
  };
  /**
   * Will throw an exception if we cant write on the tape
   */
  void isTapeWritable() const;

  /**
   * Log  m_stats  parameters into m_logContext with msg at the given level
   */
  void logWithStats(int level,const std::string& msg,
    cta::log::ScopedParamContainer& params);
  
  /**
   * Function to open the WriteSession 
   * If successful, returns a std::unique_ptr on it. A copy of that std::unique_ptr
   * will give the caller the ownership of the opened session (see unique_ptr 
   * copy constructor, which has a move semantic)
   * @return the WriteSession we need to write on tape
   */
  std::unique_ptr<castor::tape::tapeFile::WriteSession> openWriteSession();
  /**
   * Execute flush on tape, do some log and report the flush to the client
   * @param message the message the log will register
   * @param bytes the number of bytes that have been written since the last flush  
   * (for logging)
   * @param files the number of files that have been written since the last flush  
   * (also for logging)
   */
  void tapeFlush(const std::string& message,uint64_t bytes,uint64_t files,
    cta::utils::Timer & timer);
  
  /**
   * After waiting for the drive, we will dump the tape alert log content, if it
   * is not empty 
   * @return true if any critical for write alert was detected
   */
  bool logAndCheckTapeAlertsForWrite();

  virtual void run() ;
  
  //m_filesBeforeFlush and m_bytesBeforeFlush are thresholds for flushing 
  //the first one crossed will trigger the flush on tape
  
  ///how many file written before flushing on tape
  const uint64_t m_filesBeforeFlush;
  
  ///how many bytes written before flushing on tape
  const uint64_t m_bytesBeforeFlush;

  ///an interface for manipulating all type of drives
  castor::tape::tapeserver::drive::DriveInterface& m_drive;
  
  ///the object that will send reports to the client
  MigrationReportPacker & m_reportPacker;
  
  /**
   * the last fseq that has been written on the tape = the starting point 
   * of our session. The last Fseq is computed by subtracting 1 to fSeg
   * of the first file to migrate we receive. That part is done by the 
   * MigrationTaskInjector.::synchronousInjection. Thus, we compute it into 
   * that function and retrieve/set it within DataTransferSession executeWrite
   * after we make sure synchronousInjection returned true. 
   * 
   * It should be const, but it cant 
   * (because there is no mutable function member in c++)
   */
   uint64_t m_lastFseq;

  /**
   * Should the compression be enabled ? This is currently hard coded to true 
   */
  const bool m_compress;
  
  /**
   * The boolean variable describing to use on not to use Logical
   * Block Protection.
   */
  const bool m_useLbp;
  
  /**
   * Reference to the watchdog, used in run()
   */
  MigrationWatchDog & m_watchdog;
  
  /**
   * Reference to the archive mount object that
   * stores the virtual organization (vo) of the tape, the tape pool in which the tape is
   * and the density of the tape
   */
  const cta::ArchiveMount & m_archiveMount;
  
protected:
  /***
   * Helper virtual function to access the watchdog from parent class
   */
  virtual void countTapeLogError(const std::string & error) { 
    m_watchdog.addToErrorCount(error);
  }

  /**
   * Logs SCSI metrics for write session.
   */
  virtual void logSCSIMetrics();
  
private:
  /**
   *  Pointer to the task injector allowing termination signaling 
   */
  MigrationTaskInjector* m_injector;

}; // class TapeWriteSingleThread

} // namespace daemon
} // namespace tapeserver
} // namsepace tape
} // namespace castor
