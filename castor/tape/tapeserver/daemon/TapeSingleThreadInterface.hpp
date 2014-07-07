/* 
 * File:   TapeSingleThreadInterface.hpp
 * Author: dcome
 *
 * Created on March 18, 2014, 4:28 PM
 */

#pragma once

#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/daemon/CapabilityUtils.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/tapeserver/daemon/SessionStats.hpp"

namespace castor {

namespace tape {
namespace tapeserver {
namespace daemon {
  // Forward declaration
  class TapeServerReporter;
  /** 
   * This class is the base class for the 2 classes that will be executing 
   * all tape-{read|write} tasks. The template parameter Task is the type of 
   * task we are expecting : TapeReadTask or TapeWriteTask
   */
template <class Task>
class TapeSingleThreadInterface : private castor::tape::threading::Thread
{
private :
  /**
   * Utility to change the capabilities of the current tape thread
   */
  CapabilityUtils &m_capUtils;
protected:
  ///the queue of tasks 
  castor::tape::threading::BlockingQueue<Task *> m_tasks;
  
  /**
   * An interface to manipulate the drive to manipulate the tape
   * with the requested vid 
   */
  castor::tape::drives::DriveInterface & m_drive;
  
  /** Reference to the mount interface */
  castor::legacymsg::RmcProxy & m_rmc;
  
  /** Reference to the Global reporting interface */
  TapeServerReporter & m_tsr;
  
  ///The volumeID of the tape on which we want to operate  
  const std::string m_vid;

  ///log context, for ... logging purpose, copied du to thread mechanism 
  castor::log::LogContext m_logContext;
  
  client::ClientInterface::VolumeInfo m_volInfo;
  
  /**
   * Integer to notify the tapeserver if the drive has to be put down or not.
   * 0 means everything all right, any other value means we have to set it to 
   * down at the end of the session.
   */
  int m_hardarwareStatus;
  
  /** Session statistics */
  SessionStats m_stats;
 
  /**
   * This function will try to set the cap_sys_rawio capability that is needed
   * for by tape thread to access /dev/nst
   */
  void setCapabilities(){
    try {
      m_capUtils.capSetProcText("cap_sys_rawio+ep");
      log::LogContext::ScopedParam sp(m_logContext,
              log::Param("capabilities", m_capUtils.capGetProcText()));
      m_logContext.log(LOG_INFO, "Set process capabilities for using tape");
    } catch(const castor::exception::Exception &ne) {
      m_logContext.log(LOG_ERR,
              "Failed to set process capabilities for using the tape ");
    }
  }
  
  /**
   * Try to mount the tape, get an exception if it fails 
   */
  void mountTape(castor::legacymsg::RmcProxy::MountMode mode){
    castor::log::ScopedParamContainer scoped(m_logContext); 
    scoped.add("vid",m_volInfo.vid)
          .add("drive_Slot",m_drive.librarySlot);
    try {
      tape::utils::Timer timer;
        m_rmc.mountTape(m_volInfo.vid, m_drive.librarySlot,
                mode);
        const std::string modeAsString = std::string("R")+ ((mode==legacymsg::RmcProxy::MOUNT_MODE_READWRITE) ? "W" : "");
        scoped.add("RMCMountTime",timer.usecs()).add("mode",modeAsString);
        m_logContext.log(LOG_INFO, "Tape Mounted");
        
    }
    catch (castor::exception::Exception & ex) {
      scoped.add("exception_message", ex.getMessageValue())
            .add("exception_code",ex.code());
      m_logContext.log(LOG_ERR, "Failed to mount the tape");
      throw;
    }
  }
  
  /**
   * After mounting the tape, the drive will say it has no tape inside,
   * because there was no tape the first time it was opened... 
   * That function will wait a certain amount of time for the drive 
   * to tell us he acknowledge it has indeed a tap (get an ex exception in 
   * case of timeout)
   */
  void waitForDrive(){
    try{
      tape::utils::Timer timer;
      // wait 600 drive is ready
      m_drive.waitUntilReady(600);
      log::LogContext::ScopedParam sp0(m_logContext, log::Param("loadTime", timer.usecs()));
    }catch(const castor::exception::Exception& e){
      log::LogContext::ScopedParam sp01(m_logContext, log::Param("exception_code", e.code()));
      log::LogContext::ScopedParam sp02(m_logContext, log::Param("exception_message", e.getMessageValue()));
      m_logContext.log(LOG_INFO, "waiting for drive to be ready but got timeout");
      throw;
    }
  }
public:
  
  int getHardwareStatus() const {
    return m_hardarwareStatus;
  }
  /**
   * Push into the class a sentinel value to trigger to end the the thread.
   */
  void finish() { m_tasks.push(NULL); }
  
  /**
   * Push a new task into the internal queue
   * @param t the task to push
   */
  void push(Task * t) { m_tasks.push(t); }
  
  /**
   * Start the threads
   */
  virtual void startThreads(){ start(); }
  
  /**
   *  Wait for the thread to finish
   */
  virtual void waitThreads() { wait(); }
  
  /**
   * Allows to pre-set the time spent waiting for instructions, spent before
   * the tape thread is started. This is for timing the synchronous task 
   * injection done before session startup.
   * This function MUST be called before starting the thread.
   * @param secs time in seconds (double)
   */
  virtual void setWaitForInstructionsTime(double secs) { 
    m_stats.waitInstructionsTime = secs; 
  }

  /**
   * Constructor
   * @param drive An interface to manipulate the drive to manipulate the tape
   * with the requested vid
   * @param rmc The media changer (=robot) that will (un)load/(un)mount the tape
   * @param gsr
   * @param volInfo All we need to know about the tape we are manipulating 
   * @param capUtils
   * @param lc lc The log context, later on copied
   */
  TapeSingleThreadInterface(castor::tape::drives::DriveInterface & drive,
    castor::legacymsg::RmcProxy & rmc,
    TapeServerReporter & tsr,
    const client::ClientInterface::VolumeInfo& volInfo,
    CapabilityUtils &capUtils,castor::log::LogContext & lc):m_capUtils(capUtils),
    m_drive(drive), m_rmc(rmc), m_tsr(tsr), m_vid(volInfo.vid), m_logContext(lc),
    m_volInfo(volInfo),m_hardarwareStatus(0) {}
};

}
}
}
}

