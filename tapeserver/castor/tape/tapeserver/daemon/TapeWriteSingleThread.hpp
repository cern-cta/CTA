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

#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/Exceptions.hpp"
#include "castor/tape/tapeserver/file/FileWriter.hpp"
#include "castor/tape/tapeserver/file/WriteSession.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/Timer.hpp"

#include <iostream>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

// forward declaration
class TapeSessionReporter;

class MigrationTaskInjector;

/**
 * This class will execute the different tape write tasks.
 *
 */
class TapeWriteSingleThread : public TapeSingleThreadInterface<TapeWriteTask> {
public:
  /**
   * Constructor
   */
  TapeWriteSingleThread(
    castor::tape::tapeserver::drive::DriveInterface& drive,
    cta::mediachanger::MediaChangerFacade& mediaChanger,
    TapeSessionReporter& reporter,
    MigrationWatchDog& watchdog,
    const VolumeInfo& volInfo,
    cta::log::LogContext& logContext,
    MigrationReportPacker& reportPacker,
    cta::server::ProcessCap& capUtils,
    uint64_t filesBeforeFlush,
    uint64_t bytesBeforeFlush,
    const bool useLbp,
    const bool useEncryption,
    const std::string& externalEncryptionKeyScript,
    const cta::ArchiveMount& archiveMount,
    const uint64_t tapeLoadTimeout,
    cta::catalogue::Catalogue& catalogue);

  /**
   * Sets up the pointer to the task injector. This cannot be done at
   * construction time as both task injector and tape write single thread refer to
   * each other. This function should be called before starting the threads.
   * This is used for signalling problems during mounting. After that, each
   * tape write task does the signalling itself, either on tape problem, or
   * when receiving an error from the disk tasks via memory blocks.
   * @param injector pointer to the task injector
   */
  void setTaskInjector(MigrationTaskInjector *injector) {
    m_taskInjector = injector;
  }

  /**
   * 
   * @param lastFseq
   */
  void setlastFseq(uint64_t lastFseq);

private:

  // RAII class for cleaning tape stuff
  class TapeCleaning {
    TapeWriteSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    cta::utils::Timer& m_timer;
  public:
    TapeCleaning(TapeWriteSingleThread& parent, cta::utils::Timer& timer) :
      m_this(parent), m_timer(timer) {}

    ~TapeCleaning();
  };

  /**
   * Will throw an exception if we cant write on the tape
   */
  void isTapeWritable() const;

  /**
   * Log m_stats parameters into m_logContext with msg at the given level
   */
  void logWithStats(int level, const std::string& msg,
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
  void tapeFlush(const std::string& message, uint64_t bytes, uint64_t files,
                 cta::utils::Timer& timer);

  /**
   * After waiting for the drive, we will dump the tape alert log content, if it
   * is not empty 
   * @return true if any critical for write alert was detected
   */
  bool logAndCheckTapeAlertsForWrite();

  void run() override;

  //m_filesBeforeFlush and m_bytesBeforeFlush are thresholds for flushing 
  //the first one crossed will trigger the flush on tape

  ///how many file written before flushing on tape
  const uint64_t m_filesBeforeFlush;

  ///how many bytes written before flushing on tape
  const uint64_t m_bytesBeforeFlush;

  ///an interface for manipulating all type of drives
  castor::tape::tapeserver::drive::DriveInterface& m_drive;

  ///the object that will send reports to the client
  MigrationReportPacker& m_reportPacker;

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
  MigrationWatchDog& m_watchdog;

  /**
   * Reference to the archive mount object that
   * stores the virtual organization (vo) of the tape, the tape pool in which the tape is
   * and the density of the tape
   */
  const cta::ArchiveMount& m_archiveMount;

  /**
   * Reference to the catalogue. It is only used in EncryptionControl to modify tape information
   */
  cta::catalogue::Catalogue& m_catalogue;

protected:
  /***
   * Helper virtual function to access the watchdog from parent class
   */
  void countTapeLogError(const std::string& error) override {
    m_watchdog.addToErrorCount(error);
  }

  /**
   * Logs SCSI metrics for write session.
   */
  void logSCSIMetrics() override;

private:
  /**
   *  Pointer to the task injector allowing termination signaling 
   */
  MigrationTaskInjector *m_taskInjector;

}; // class TapeWriteSingleThread

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
