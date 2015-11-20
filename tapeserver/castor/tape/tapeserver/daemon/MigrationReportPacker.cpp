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

#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "serrno.h"

#include <memory>
#include <numeric>
#include <cstdio>

namespace{
  struct failedMigrationRecallResult : public castor::exception::Exception{
    failedMigrationRecallResult(const std::string& s): Exception(s){}
  };
}
using castor::log::LogContext;
using castor::log::Param;

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
MigrationReportPacker::MigrationReportPacker(cta::ArchiveMount *archiveMount,
  castor::log::LogContext & lc):
ReportPackerInterface<detail::Migration>(lc),
m_workerThread(*this),m_errorHappened(false),m_continue(true), m_archiveMount(archiveMount) {
}
//------------------------------------------------------------------------------
//Destructore
//------------------------------------------------------------------------------
MigrationReportPacker::~MigrationReportPacker(){
  castor::server::MutexLocker ml(&m_producterProtection);
}
//------------------------------------------------------------------------------
//reportCompletedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportCompletedJob(
std::unique_ptr<cta::ArchiveJob> successfulArchiveJob) {
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulArchiveJob)));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob,
        const castor::exception::Exception &ex){
  std::unique_ptr<Report> rep(new ReportError(std::move(failedArchiveJob),ex));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFlush
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFlush(drive::compressionStats compressStats){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportFlush(compressStats));
}
//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportEndOfSession() {
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSession());
}
//------------------------------------------------------------------------------
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportEndOfSessionWithErrors(std::string msg,int errorCode){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,errorCode));
}
//------------------------------------------------------------------------------
//synchronousReportEndWithErrors
//------------------------------------------------------------------------------ 
void MigrationReportPacker::synchronousReportEndWithErrors(const std::string msg, int errorCode){
  // We create the report task here and excute it immediately instead of posting
  // it to a queue.
  ReportEndofSessionWithErrors rep(msg,errorCode);
  rep.execute(*this);
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSuccessful::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_successfulArchiveJobs.push(std::move(m_successfulArchiveJob));
}

//------------------------------------------------------------------------------
//ReportFlush::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& reportPacker){
  if(!reportPacker.m_errorHappened){
    // We can receive double flushes when the periodic flush happens
    // right before the end of session (which triggers also a flush)
    // We refrain from sending an empty report to the client in this case.
    if (reportPacker.m_successfulArchiveJobs.empty()) {
      reportPacker.m_lc.log(LOG_INFO,"Received a flush report from tape, but had no file to report to client. Doing nothing.");
      return;
    }
    try{
      while(!reportPacker.m_successfulArchiveJobs.empty()) {
        std::unique_ptr<cta::ArchiveJob> job(std::move(reportPacker.m_successfulArchiveJobs.front()));
        job->complete();
        reportPacker.m_successfulArchiveJobs.pop();
      }
      reportPacker.m_lc.log(LOG_INFO,"Reported to the client that a batch of files was written on tape");
    }
    catch(const castor::exception::Exception& e){
      LogContext::ScopedParam sp[]={
        LogContext::ScopedParam(reportPacker.m_lc, Param("exceptionCode",e.code())),
        LogContext::ScopedParam(reportPacker.m_lc, Param("exceptionMessageValue", e.getMessageValue())),
        LogContext::ScopedParam(reportPacker.m_lc, Param("exceptionWhat",e.what()))
      };
      tape::utils::suppresUnusedVariable(sp);
      
      const std::string msg_error="An exception was caught trying to call reportMigrationResults";
      reportPacker.m_lc.log(LOG_ERR,msg_error);
      throw failedMigrationRecallResult(msg_error);
    }
  } else {
    // This is an abnormal situation: we should never flush after an error!
    reportPacker.m_lc.log(LOG_ALERT,"Received a flush after an error: sending file errors to client");
  }
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& reportPacker){
  if(!reportPacker.m_errorHappened){
    reportPacker.m_archiveMount->complete();
    log::ScopedParamContainer sp(reportPacker.m_lc);
    reportPacker.m_lc.log(LOG_INFO,"Reported end of session to client");
    if(reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(log::Param("status","success"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
  else {
    // We have some errors
    reportPacker.m_archiveMount->complete();
    log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", "Previous file errors")
      .add("errorCode", SEINTERNAL);
    reportPacker.m_lc.log(LOG_ERR,"Reported end of session with error to client due to previous file errors");
    if(reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(log::Param("status","failure"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
  reportPacker.m_continue=false;
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& reportPacker){
  if(reportPacker.m_errorHappened) {
    reportPacker.m_archiveMount->complete();
    log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", m_message)
      .add("errorCode", m_errorCode);
    reportPacker.m_lc.log(LOG_INFO,"Reported end of session with error to client after sending file errors");
  } else{
    const std::string& msg ="Reported end of session with error to client";
    // As a measure of safety we censor any session error which is not ENOSPC into
    // SEINTERNAL. ENOSPC is the only one interpreted by the tape gateway.
    if (ENOSPC != m_errorCode) {
      m_errorCode = SEINTERNAL;
    }
    reportPacker.m_archiveMount->complete(); 
    reportPacker.m_lc.log(LOG_INFO,msg);
  }
  if(reportPacker.m_watchdog) {
    reportPacker.m_watchdog->addParameter(log::Param("status",
      ENOSPC == m_errorCode?"success":"failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500*1000);
  }
  reportPacker.m_continue=false;
}
//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportError::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_errorHappened=true;
  reportPacker.m_lc.log(LOG_ERR,m_ex.getMessageValue());
  m_failedArchiveJob->failed(cta::exception::Exception(m_ex.getMessageValue()));
}

//------------------------------------------------------------------------------
//WorkerThread::WorkerThread
//------------------------------------------------------------------------------
MigrationReportPacker::WorkerThread::WorkerThread(MigrationReportPacker& parent):
m_parent(parent) {
}
//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void MigrationReportPacker::WorkerThread::run(){
  m_parent.m_lc.pushOrReplace(log::Param("thread", "ReportPacker"));
  try{
    while(m_parent.m_continue) {
      std::unique_ptr<Report> rep (m_parent.m_fifo.pop());
      try{
        rep->execute(m_parent);
      }
      catch(const failedMigrationRecallResult& e){
        //here we catch a failed report MigrationResult. We try to close and if that fails too
        //we end up in the catch below
        m_parent.m_archiveMount->complete();
        m_parent.m_lc.log(LOG_INFO,"Successfully closed client's session after the failed report MigrationResult");
        if (m_parent.m_watchdog) {
          m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
          m_parent.m_watchdog->addParameter(log::Param("status","failure"));
        }
        break;
      }
    }
  }
  catch(const castor::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report endOfSession or endofSessionWithErrors and got a castor exception, cant do much more. The exception is the following: " << e.getMessageValue();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(const cta::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report endOfSession or endofSessionWithErrors and got a CTA exception, cant do much more. The exception is the following: " << e.getMessageValue();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(const std::exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report endOfSession or endofSessionWithErrors and got a standard exception, cant do much more. The exception is the following: " << e.what();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(...){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report endOfSession or endofSessionWithErrors and got an unknown exception, cant do much more.";
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  }
  // Drain the FIFO if necessary. We know that m_continue will be 
  // set by ReportEndofSessionWithErrors or ReportEndofSession
  // TODO devise a more generic mechanism
  while(m_parent.m_continue) {
    std::unique_ptr<Report> rep (m_parent.m_fifo.pop());
    if (dynamic_cast<ReportEndofSessionWithErrors *>(rep.get())  || 
        dynamic_cast<ReportEndofSession *>(rep.get()))
      m_parent.m_continue = false;
  }
}

}}}}
