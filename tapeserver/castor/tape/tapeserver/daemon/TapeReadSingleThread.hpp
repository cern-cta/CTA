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

#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeReadTask.hpp"
#include "common/threading/Thread.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/RecallTaskInjector.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "common/Timer.hpp"

#include <iostream>
#include <memory>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

//forward declaration
class TapeServerReporter;

  /**
   * This class will execute the different tape read tasks.
   * 
   */
class TapeReadSingleThread : public TapeSingleThreadInterface<TapeReadTask>{
public:
  /**
   * Constructor:
   */
  TapeReadSingleThread(castor::tape::tapeserver::drive::DriveInterface & drive,
          cta::mediachanger::MediaChangerFacade &mc,
          TapeServerReporter & initialProcess,
          const VolumeInfo& volInfo, 
          uint64_t maxFilesRequest,
          cta::server::ProcessCap &capUtils,
          RecallWatchDog& watchdog,
          cta::log::LogContext & lc,
          RecallReportPacker &rrp,
          const bool useLbp,
          const bool useRAO,
          const bool useEncryption,
          const std::string & externalEncryptionKeyScript,
          const cta::RetrieveMount &retrieveMount,
          const uint32_t tapeLoadTimeout);
   
  /**
   * Set the task injector. Has to be done that way (and not in the constructor)
   *  because there is a dependency
   * @param ti the task injector
   */
  void setTaskInjector(RecallTaskInjector * ti) {
    m_taskInjector = ti;
  }

private:  
  
  /**
   * Returns the string representation of the specified mount type
   */
  const char *mountTypeToString(const cta::common::dataStructures::MountType mountType) const
    throw();
  
  //RAII class for cleaning tape stuff
  class TapeCleaning{
    TapeReadSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    cta::utils::Timer & m_timer;
  public:
    TapeCleaning(TapeReadSingleThread& parent, cta::utils::Timer & timer):
      m_this(parent), m_timer(timer){}
    ~TapeCleaning();
  };
  /**
   * Pop a task from its tasks and if there is not enough tasks left, it will 
   * ask the task injector for more 
   * @return m_tasks.pop();
   */
  TapeReadTask * popAndRequestMoreJobs();
    
    /**
     * Try to open an tapeFile::ReadSession, if it fails, we got an exception.
     * Return an std::unique_ptr will ensure the callee will have the ownershipe 
     * of the object through unique_ptr's copy constructor
     * @return 
     */
  std::unique_ptr<castor::tape::tapeFile::ReadSession> openReadSession();

  /**
   * This function is from Thread, it is the function that will do all the job
   */
  virtual void run();

  /**
   * Log msg with the given level, Session time is the time taken by the action 
   * @param level
   * @param msg
   * @param sessionTime
   */
  void logWithStat(int level,const std::string& msg,
    cta::log::ScopedParamContainer& params);
  
  /**
   * Number of files a single request to the client might give us.
   * Used in the loop-back function to ask the task injector to request more job
   */
  const uint64_t m_maxFilesRequest;
  
  ///a pointer to task injector, thus we can ask him for more tasks
  castor::tape::tapeserver::daemon::RecallTaskInjector * m_taskInjector;
  
  /// Reference to the watchdog, used in run()
  RecallWatchDog& m_watchdog;
  
  /// Reference to the RecallReportPacker, used to update tape/drive state during recall
  RecallReportPacker & m_rrp;
  
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
  
  /// Helper virtual function to access the watchdog from parent class
  virtual void countTapeLogError(const std::string & error) { 
    m_watchdog.addToErrorCount(error);
  }
  
protected:
  /**
   * Logs SCSI metrics for read session.
   */
  virtual void logSCSIMetrics();
}; // class TapeReadSingleThread

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
