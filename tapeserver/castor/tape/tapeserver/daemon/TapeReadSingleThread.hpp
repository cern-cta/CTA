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

#include <stdio.h>

#include <iostream>
#include <memory>

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "common/threading/Thread.hpp"
#include "common/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//forward declaration
class TapeSessionReporter;

class RecallTaskInjector;

/**
 * This class will execute the different tape read tasks.
 *
 */
class TapeReadSingleThread : public TapeSingleThreadInterface<TapeReadTask> {
public:
  /**
   * Constructor
   */
  TapeReadSingleThread(castor::tape::tapeserver::drive::DriveInterface& drive,
                       cta::mediachanger::MediaChangerFacade& mediaChanger,
                       TapeSessionReporter& reporter,
                       const VolumeInfo& volInfo,
                       uint64_t maxFilesRequest,
                       cta::server::ProcessCap& capUtils,
                       RecallWatchDog& watchdog,
                       cta::log::LogContext& logContext,
                       RecallReportPacker& reportPacker,
                       const bool useLbp,
                       const bool useRAO,
                       const bool useEncryption,
                       const std::string& externalEncryptionKeyScript,
                       const cta::RetrieveMount& retrieveMount,
                       const uint32_t tapeLoadTimeout,
                       cta::catalogue::Catalogue& catalogue);

  /**
   * Sets up the pointer to the task injector. This cannot be done at
   * construction time as both task injector and tape write single thread refer to
   * each other. This function should be called before starting the threads.
   * This is used for signalling problems during mounting. After that, each
   * tape write task does the signalling itself, either on tape problem, or
   * when receiving an error from the disk tasks via memory blocks.
   * @param injector the task injector
   */
  void setTaskInjector(RecallTaskInjector *injector) {
    m_taskInjector = injector;
  }

private:

  // RAII class for cleaning tape stuff
  class TapeCleaning {
    TapeReadSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    cta::utils::Timer& m_timer;
  public:
    TapeCleaning(TapeReadSingleThread& parent, cta::utils::Timer& timer) :
      m_this(parent), m_timer(timer) {}

    ~TapeCleaning();
  };

  /**
   * Pop a task from its tasks and if there is not enough tasks left, it will 
   * ask the task injector for more 
   * @return m_tasks.pop();
   */
  TapeReadTask *popAndRequestMoreJobs();

  /**
   * Try to open an tapeFile::ReadSession, if it fails, we got an exception.
   * Return an std::unique_ptr will ensure the callee will have the ownership
   * of the object through unique_ptr's copy constructor
   * @return
   */
  std::unique_ptr<castor::tape::tapeFile::ReadSession> openReadSession();

  /**
   * This function is from Thread, it is the function that will do all the job
   */
  void run() override;

  /**
   * Log m_stats parameters into m_logContext with msg at the given level
   */
  void logWithStat(int level, const std::string& msg,
                   cta::log::ScopedParamContainer& params);

  /**
   * Number of files a single request to the client might give us.
   * Used in the loop-back function to ask the task injector to request more job
   */
  const uint64_t m_maxFilesRequest;

  ///a pointer to task injector, thus we can ask him for more tasks
  RecallTaskInjector *m_taskInjector{};

  /// Reference to the watchdog, used in run()
  RecallWatchDog& m_watchdog;

  /// Reference to the RecallReportPacker, used to update tape/drive state during recall
  RecallReportPacker& m_reportPacker;

  /**
   * The boolean variable describing to use on not to use Logical
   * Block Protection.
   */
  const bool m_useLbp;

  /**
   * The boolean variable describing to use on not to use Recommended
   * Access Order
   */
  bool m_useRAO;

  /**
   * The retrieve mount object to get the VO, the tape pool and the density of the tape
   * on which we are reading
   */
  const cta::RetrieveMount& m_retrieveMount;

  /**
   * Reference to the catalogue. It is only used in EncryptionControl to modify tape information
   */
  cta::catalogue::Catalogue& m_catalogue;

  /// Helper virtual function to access the watchdog from parent class
  void countTapeLogError(const std::string& error) override {
    m_watchdog.addToErrorCount(error);
  }

protected:
  /**
   * Logs SCSI metrics for read session.
   */
  void logSCSIMetrics() override;
}; // class TapeReadSingleThread

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
