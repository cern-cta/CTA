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
#include <iostream>
#include <stdio.h>
#include "castor/tape/tapeserver/daemon/GlobalStatusReporter.hpp"
#include "castor/tape/tapeserver/daemon/CapabilityUtils.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
// forward declaration
class GlobalStatusReporter;

class TapeWriteSingleThread :  public TapeSingleThreadInterface<TapeWriteTask> {
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
          GlobalStatusReporter & gsr,
          const client::ClientInterface::VolumeInfo& volInfo,
          castor::log::LogContext & lc, MigrationReportPacker & repPacker,
           CapabilityUtils &capUtils,
	  uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush): 
  TapeSingleThreadInterface<TapeWriteTask>(drive, rmc, gsr, volInfo,capUtils, lc),
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
  public:
    TapeCleaning(TapeWriteSingleThread& parent):m_this(parent){}
    ~TapeCleaning(){
      try{
      // Do the final cleanup
      m_this.m_drive.unloadTape();
      m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread : Tape unloaded");
      // And return the tape to the library
      m_this.m_rmc.unmountTape(m_this.m_volInfo.vid, m_this.m_drive.librarySlot);
      m_this.m_logContext.log(LOG_INFO, "TapeWriteSingleThread : tape unmounted");
      m_this.m_gsr.tapeUnmounted();
              
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
      m_this.m_gsr.finish();
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
   * Execute flush on tape
   * @param message the message the log will register
   * @param bytes the number of bytes that have been written since the last flush  
   * (for logging)
   * @param files the number of files that have been written since the last flush  
   * (also for logging)
   */
  void tapeFlush(const std::string& message,uint64_t bytes,uint64_t files){
    m_drive.flush();
    log::LogContext::ScopedParam sp0(m_logContext, log::Param("files", files));
    log::LogContext::ScopedParam sp1(m_logContext, log::Param("bytes", bytes));
    m_logContext.log(LOG_INFO,message);
    m_reportPacker.reportFlush();
  }

    void mountTape(){
    castor::log::ScopedParamContainer scoped(m_logContext); 
    scoped.add("vid",m_volInfo.vid)
          .add("drive_Slot",m_drive.librarySlot);
    try {
         m_rmc.mountTape(m_volInfo.vid, m_drive.librarySlot, legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
        m_logContext.log(LOG_INFO, "Tape Mounted");
    }
    catch (castor::exception::Exception & ex) {
      scoped.add("exception_message", ex.getMessageValue())
            .add("exception_code",ex.code());
      m_logContext.log(LOG_ERR, "Failed to mount the tape for writing");
      throw;
    }
  }
    
    void waitForDrive(){
      try{
        // wait 600 drive is ready
        m_drive.waitUntilReady(600);
      }catch(const castor::exception::Exception& e){
        log::LogContext::ScopedParam sp01(m_logContext, log::Param("exception_code", e.code()));
        log::LogContext::ScopedParam sp02(m_logContext, log::Param("exception_message", e.getMessageValue()));
        m_logContext.log(LOG_INFO, "TapeWriteSingleThread waiting for drive to be ready timeout");
        throw;
      }
    }
    
  virtual void run() {

    try
    {
      m_logContext.pushOrReplace(log::Param("thread", "TapeWrite"));
      setCapabilities();
      
      TapeCleaning cleaner(*this);
      mountTape();
      waitForDrive();
      // Then we have to initialise the tape write session
      std::auto_ptr<castor::tape::tapeFile::WriteSession> writeSession(openWriteSession());
      
      //log and notify
      m_logContext.log(LOG_INFO, "Starting tape write thread");
      m_gsr.tapeMountedForWrite();
      uint64_t bytes=0;
      uint64_t files=0;
      std::auto_ptr<TapeWriteTask> task ;      
      while(1) {
        
        //get a task
        task.reset(m_tasks.pop());
        
        //if is the end
        if(NULL==task.get()) {      
          //we flush without asking
          tapeFlush("End of TapeWriteWorkerThread::run() flushing",bytes,files);
          //end of session + log 
          m_reportPacker.reportEndOfSession();
          m_logContext.log(LOG_DEBUG, "Finishing tape write thread");
          break;
        }
        
        task->execute(*writeSession,m_reportPacker,m_logContext);

        //raise counters
        files++;
        bytes+=task->fileSize();
          
        //if one counter is above a threshold, then we flush
        if (files >= m_filesBeforeFlush || bytes >= m_bytesBeforeFlush) {
          tapeFlush("Normal flush because thresholds was reached",bytes,files);
          files=0;
          bytes=0;
        }
      } //end of while(1))
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
   * that function and retrieve/set it within MountSession executeWrite
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
