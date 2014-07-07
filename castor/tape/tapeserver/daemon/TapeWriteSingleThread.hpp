/******************************************************************************
 *                      TapeWriteSingleThread.hpp
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

#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/TapeWriteTask.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/drive/Drive.hpp"
#include "castor/tape/tapeserver/daemon/TapeSingleThreadInterface.hpp"
#include "castor/legacymsg/RmcProxy.hpp"
#include "castor/tape/tapeserver/daemon/TapeServerReporter.hpp"
#include "castor/tape/tapeserver/daemon/CapabilityUtils.hpp"
#include "castor/tape/utils/Timer.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include <iostream>
#include <stdio.h>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
// forward declaration
class TapeServerReporter;

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
   */
  TapeWriteSingleThread(castor::tape::drives::DriveInterface & drive, 
          castor::legacymsg::RmcProxy & rmc,
          TapeServerReporter & tsr,
          const client::ClientInterface::VolumeInfo& volInfo,
          castor::log::LogContext & lc, MigrationReportPacker & repPacker,
           CapabilityUtils &capUtils,
	  uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush): 
  TapeSingleThreadInterface<TapeWriteTask>(drive, rmc, tsr, volInfo,capUtils, lc),
          m_filesBeforeFlush(filesBeforeFlush),
          m_bytesBeforeFlush(bytesBeforeFlush),
          m_drive(drive), m_reportPacker(repPacker),
          m_lastFseq(-1),
          m_compress(true) {}
    
  /**
   * 
   * @param lastFseq
   */
  void setlastFseq(uint64_t lastFseq){
    m_lastFseq=lastFseq;
  }
private:
    class TapeCleaning{
    TapeWriteSingleThread& m_this;
    // As we are living in the single thread of tape, we can borrow the timer
    utils::Timer & m_timer;
  public:
    TapeCleaning(TapeWriteSingleThread& parent, utils::Timer & timer):
      m_this(parent), m_timer(timer){}
    ~TapeCleaning(){
      try{
        // Do the final cleanup
        // in the special case of a "manual" mode tape, we should skip the unload too.
        if (m_this.m_drive.librarySlot != "manual") {
          m_this.m_drive.unloadTape();
          m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread: Tape unloaded");
        } else {
          m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread: Tape NOT unloaded (manual mode)");
        }
        m_this.m_stats.unloadTime += m_timer.secs(utils::Timer::resetCounter);
        // And return the tape to the library
        // In case of manual mode, this will be filtered by the rmc daemon
        // (which will do nothing)
        m_this.m_rmc.unmountTape(m_this.m_volInfo.vid, m_this.m_drive.librarySlot);
        m_this.m_stats.unmountTime += m_timer.secs(utils::Timer::resetCounter);
        m_this.m_logContext.log(LOG_INFO, m_this.m_drive.librarySlot != "manual"?
          "TapeWriteSingleThread : tape unmounted":"TapeWriteSingleThread : tape NOT unmounted (manual mode)");
        m_this.m_tsr.tapeUnmounted();
        m_this.m_stats.waitReportingTime += m_timer.secs(utils::Timer::resetCounter);
      }
      catch(const castor::exception::Exception& ex){
        //set it to -1 to notify something failed during the cleaning 
        m_this.m_hardarwareStatus = -1;
        castor::log::ScopedParamContainer scoped(m_this.m_logContext);
        scoped.add("exception_message", ex.getMessageValue())
        .add("exception_code",ex.code());
        m_this.m_logContext.log(LOG_ERR, "Exception in TapeWriteSingleThread-TapeCleaning");
      } catch (...) {
          //set it to -1 to notify something failed during the cleaning 
          m_this.m_hardarwareStatus = -1;
          m_this.m_logContext.log(LOG_ERR, "Non-Castor exception in TapeWriteSingleThread-TapeCleaning when unmounting the tape");
      }
      
      //then we terminate the global status reporter
      m_this.m_tsr.finish();
    }
  };
  
  /**
   * Function to open the WriteSession 
   * If successful, returns a std::auto_ptr on it. A copy of that std::auto_ptr
   * will give the caller the ownership of the opened session (see auto_ptr 
   * copy constructor, which has a move semantic)
   * @return the WriteSession we need to write on tape
   */
  std::auto_ptr<castor::tape::tapeFile::WriteSession> openWriteSession() {
    using castor::log::LogContext;
    using castor::log::Param;
    typedef LogContext::ScopedParam ScopedParam;
    
    std::auto_ptr<castor::tape::tapeFile::WriteSession> writeSession;
  
    ScopedParam sp[]={
        ScopedParam(m_logContext, Param("vid",m_vid)),
        ScopedParam(m_logContext, Param("lastFseq", m_lastFseq)),
        ScopedParam(m_logContext, Param("compression", m_compress)) 
      };
    tape::utils::suppresUnusedVariable(sp);
      try {
       writeSession.reset(
         new castor::tape::tapeFile::WriteSession(m_drive,m_volInfo,m_lastFseq,m_compress)
       );
        m_logContext.log(LOG_INFO, "Tape Write session session successfully started");
      }
      catch (castor::exception::Exception & e) {
        ScopedParam sp0(m_logContext, Param("ErrorMessage", e.getMessageValue()));
        ScopedParam sp1(m_logContext, Param("ErrorCode", e.code()));
        m_logContext.log(LOG_ERR, "Failed to start tape write session");
        // TODO: log and unroll the session
        // TODO: add an unroll mode to the tape read task. (Similar to exec, but pushing blocks marked in error)
        throw;
      }
    return writeSession;
  }
  /**
   * Execute flush on tape, do some log and report the flush to the client
   * @param message the message the log will register
   * @param bytes the number of bytes that have been written since the last flush  
   * (for logging)
   * @param files the number of files that have been written since the last flush  
   * (also for logging)
   */
  void tapeFlush(const std::string& message,uint64_t bytes,uint64_t files,
        utils::Timer & timer){
    m_drive.flush();
    double flushTime = timer.secs(utils::Timer::resetCounter);
    log::ScopedParamContainer params(m_logContext);
    params.add("files", files)
          .add("bytes", bytes)
          .add("flushTime", flushTime);
    m_logContext.log(LOG_INFO,message);
    m_reportPacker.reportFlush();
    m_stats.flushTime += flushTime;
  }
  

  virtual void run() {
    m_logContext.pushOrReplace(log::Param("thread", "TapeWrite"));
    castor::tape::utils::Timer timer, totalTimer;
    try
    {
      // Set capabilities allowing rawio (and hence arbitrary SCSI commands)
      // through the st driver file descriptor.
      setCapabilities();
      {
        //log and notify
        m_logContext.log(LOG_INFO, "Starting tape write thread");
        // The tape will be loaded 
        // it has to be unloaded, unmounted at all cost -> RAII
        // will also take care of the TapeServerReporter
        TapeCleaning cleaner(*this, timer);
        // Before anything, the tape should be mounted
        // This call does the logging of the mount
        mountTape(castor::legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
        waitForDrive();
        m_stats.mountTime += timer.secs(utils::Timer::resetCounter);
        {
          castor::log::ScopedParamContainer scoped(m_logContext);
          scoped.add("mountTime", m_stats.mountTime);
          m_logContext.log(LOG_INFO, "Tape mounted and drive ready");
        }
        // Then we have to initialise the tape write session
        std::auto_ptr<castor::tape::tapeFile::WriteSession> writeSession(openWriteSession());
        m_stats.positionTime += timer.secs(utils::Timer::resetCounter);
        {
          castor::log::ScopedParamContainer scoped(m_logContext);
          scoped.add("positionTime", m_stats.positionTime);
          m_logContext.log(LOG_INFO, "Write session initialised, tape VID checked and drive positioned for writing");
        }
        m_tsr.tapeMountedForWrite();
        uint64_t bytes=0;
        uint64_t files=0;
        m_stats.waitReportingTime += timer.secs(utils::Timer::resetCounter);
        
        std::auto_ptr<TapeWriteTask> task;    
        while(1) {
          //get a task
          task.reset(m_tasks.pop());
          m_stats.waitInstructionsTime += timer.secs(utils::Timer::resetCounter);
          //if is the end
          if(NULL==task.get()) {      
            //we flush without asking
            tapeFlush("No more data to write on tape, unconditional flushing to the client",bytes,files,timer);
            m_stats.flushTime += timer.secs(utils::Timer::resetCounter);
            //end of session + log
            m_reportPacker.reportEndOfSession();
            log::LogContext::ScopedParam sp0(m_logContext, log::Param("tapeThreadDuration", totalTimer.secs()));
            m_logContext.log(LOG_DEBUG, "writing data to tape has finished");
            break;
          }
          task->execute(*writeSession,m_reportPacker,m_logContext,m_stats,timer);
  
          // Increase local flush counters (session counters are incremented by
          // the task)
          files++;
          bytes+=task->fileSize();
          //if one flush counter is above a threshold, then we flush
          if (files >= m_filesBeforeFlush || bytes >= m_bytesBeforeFlush) {
            tapeFlush("Normal flush because thresholds was reached",bytes,files,timer);
            files=0;
            bytes=0;
          }
        } //end of while(1))
      }
      // The session completed successfully, and the cleaner (unmount) executed
      // at the end of the previous block. Log the results.
      double sessionTime = totalTimer.secs();
      log::ScopedParamContainer params(m_logContext);
      params.add("mountTime", m_stats.mountTime)
            .add("positionTime", m_stats.positionTime)
            .add("waitInstructionsTime", m_stats.waitInstructionsTime)
            .add("checksumingTime", m_stats.checksumingTime)
            .add("transferTime", m_stats.transferTime)
            .add("waitDataTime", m_stats.waitDataTime)
            .add("waitReportingTime", m_stats.waitReportingTime)
            .add("flushTime", m_stats.flushTime)
            .add("unloadTime", m_stats.unloadTime)
            .add("unmountTime", m_stats.unmountTime)
            .add("dataVolume", m_stats.dataVolume)
            .add("headerVolume", m_stats.headerVolume)
            .add("filesCount", m_stats.filesCount)
            .add("dataBandwidthMiB/s", 1.0*m_stats.dataVolume
                    /1024/1024/sessionTime)
            .add("driveBandwidthMiB/s", 1.0*(m_stats.dataVolume+m_stats.headerVolume)
                    /1024/1024/sessionTime)
            .add("sessionTime", sessionTime);
      m_logContext.log(LOG_INFO, "Completed migration session successfully");
    } //end of try
    catch(const castor::exception::Exception& e){
      //we end there because write session could not be opened or because a task failed

      //first empty all the tasks and circulate mem blocks
      while(1) {
        std::auto_ptr<TapeWriteTask>  task(m_tasks.pop());
        if(task.get()==NULL) {
          break;
        }
        task->circulateMemBlocks(m_logContext);
      }
      //then log
      log::LogContext::ScopedParam sp1(m_logContext, log::Param("exceptionCode", e.code()));
      log::LogContext::ScopedParam sp2(m_logContext, log::Param("error_MessageValue", e.getMessageValue()));
      m_logContext.log(LOG_ERR,"An error occurred during TapwWriteSingleThread::execute");
      
      m_reportPacker.reportEndOfSessionWithErrors(e.what(),e.code());
    }    
  }
  
  //m_filesBeforeFlush and m_bytesBeforeFlush are thresholds for flushing 
  //the first one crossed will trigger the flush on tape
  
  ///how many file written before flushing on tape
  const uint64_t m_filesBeforeFlush;
  
  ///how many bytes written before flushing on tape
  const uint64_t m_bytesBeforeFlush;

  ///an interface for manipulating all type of drives
  castor::tape::drives::DriveInterface& m_drive;
  
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
};
}}}}
