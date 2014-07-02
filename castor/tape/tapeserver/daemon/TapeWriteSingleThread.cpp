/******************************************************************************
 *                      TapeWriteSingleThread.cpp
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



#include "castor/tape/tapeserver/daemon/TapeWriteSingleThread.hpp"

castor::tape::tapeserver::daemon::TapeWriteSingleThread::TapeWriteSingleThread(
castor::tape::drives::DriveInterface & drive, 
        castor::legacymsg::RmcProxy & rmc,
        TapeServerReporter & tsr,
        const client::ClientInterface::VolumeInfo& volInfo,
        castor::log::LogContext & lc, MigrationReportPacker & repPacker,
        castor::server::ProcessCap &capUtils,
        uint64_t filesBeforeFlush, uint64_t bytesBeforeFlush): 
        TapeSingleThreadInterface<TapeWriteTask>(drive, rmc, tsr, volInfo,capUtils, lc),
        m_filesBeforeFlush(filesBeforeFlush),
        m_bytesBeforeFlush(bytesBeforeFlush),
        m_drive(drive), m_reportPacker(repPacker),
        m_lastFseq(-1),
        m_compress(true) {}

void castor::tape::tapeserver::daemon::TapeWriteSingleThread::
setlastFseq(uint64_t lastFseq){
  m_lastFseq=lastFseq;
}

std::auto_ptr<castor::tape::tapeFile::WriteSession> 
castor::tape::tapeserver::daemon::TapeWriteSingleThread::openWriteSession() {
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

void castor::tape::tapeserver::daemon::TapeWriteSingleThread::
tapeFlush(const std::string& message,uint64_t bytes,uint64_t files){
  m_drive.flush();
  log::LogContext::ScopedParam sp0(m_logContext, log::Param("files", files));
  log::LogContext::ScopedParam sp1(m_logContext, log::Param("bytes", bytes));
  m_logContext.log(LOG_INFO,message);
  m_reportPacker.reportFlush();
}


void castor::tape::tapeserver::daemon::TapeWriteSingleThread::run() {
  
  try
  {
    m_logContext.pushOrReplace(log::Param("thread", "TapeWrite"));
    setCapabilities();
    
    TapeCleaning cleaner(*this);
    mountTape(castor::legacymsg::RmcProxy::MOUNT_MODE_READWRITE);
    waitForDrive();
    // Then we have to initialise the tape write session
    std::auto_ptr<castor::tape::tapeFile::WriteSession> writeSession(openWriteSession());
    
    //log and notify
    m_logContext.log(LOG_INFO, "Starting tape write thread");
    m_tsr.tapeMountedForWrite();
    uint64_t bytes=0;
    uint64_t files=0;
    std::auto_ptr<TapeWriteTask> task;  
    
    tape::utils::Timer timer;
    
    while(1) {
      
      //get a task
      task.reset(m_tasks.pop());
      
      //if is the end
      if(NULL==task.get()) {      
        //we flush without asking
        tapeFlush("No more data to write on tape, unconditional flushing to the client",bytes,files);
        //end of session + log 
        m_reportPacker.reportEndOfSession();
        log::LogContext::ScopedParam sp0(m_logContext, log::Param("time taken", timer.secs()));
        m_logContext.log(LOG_DEBUG, "writing data to tape has finished");
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
    //we end there because write session could not be opened 
    //or because a task failed
    
    //first empty all the tasks and circulate mem blocks
    while(1) {
      std::auto_ptr<TapeWriteTask>  task(m_tasks.pop());
      if(task.get()==NULL) {
        break;
      }
      task->circulateMemBlocks();
    }
    //then log
    log::LogContext::ScopedParam sp1(m_logContext, log::Param("exceptionCode", e.code()));
    log::LogContext::ScopedParam sp2(m_logContext, log::Param("error_MessageValue", e.getMessageValue()));
    m_logContext.log(LOG_ERR,"An error occurred during TapwWriteSingleThread::execute");
    
    m_reportPacker.reportEndOfSessionWithErrors(e.what(),e.code());
  }
  
}