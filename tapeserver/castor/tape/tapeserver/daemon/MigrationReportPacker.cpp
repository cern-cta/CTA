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
#include "catalogue/TapeFileWritten.hpp"

#include <memory>
#include <numeric>
#include <cstdio>

using cta::log::LogContext;
using cta::log::Param;

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
MigrationReportPacker::MigrationReportPacker(cta::ArchiveMount *archiveMount,
  cta::log::LogContext & lc):
ReportPackerInterface<detail::Migration>(lc),
m_workerThread(*this),m_errorHappened(false),m_continue(true), m_archiveMount(archiveMount) {
}
//------------------------------------------------------------------------------
//Destructor
//------------------------------------------------------------------------------
MigrationReportPacker::~MigrationReportPacker(){
  cta::threading::MutexLocker ml(m_producterProtection);
}
//------------------------------------------------------------------------------
//reportCompletedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportCompletedJob(
std::unique_ptr<cta::ArchiveJob> successfulArchiveJob, cta::log::LogContext & lc) {
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulArchiveJob)));
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportSuccessful");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportCompletedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob,
        const cta::exception::Exception &ex, cta::log::LogContext & lc){
  std::unique_ptr<Report> rep(new ReportError(std::move(failedArchiveJob),ex));
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportError");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFailedJob(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFlush
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFlush(drive::compressionStats compressStats, cta::log::LogContext & lc){
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportFlush");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportFlush(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportFlush(compressStats));
}
//------------------------------------------------------------------------------
//reportTapeFull
//------------------------------------------------------------------------------
void MigrationReportPacker::reportTapeFull(cta::log::LogContext & lc){
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportTapeFull");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportTapeFull(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportTapeFull());
}
//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportEndOfSession(cta::log::LogContext & lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSession");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportEndOfSession(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportEndofSession());
}
//------------------------------------------------------------------------------
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportEndOfSessionWithErrors(std::string msg,int errorCode, cta::log::LogContext & lc){
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSessionWithErrors");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportEndOfSessionWithErrors(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,errorCode));
}

//------------------------------------------------------------------------------
//reportTestGoingToEnd
//------------------------------------------------------------------------------
void MigrationReportPacker::reportTestGoingToEnd(cta::log::LogContext & lc){
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportTestGoingToEnd");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportTestGoingToEnd(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportTestGoingToEnd());
}

//------------------------------------------------------------------------------
//synchronousReportEndWithErrors
//------------------------------------------------------------------------------ 
void MigrationReportPacker::synchronousReportEndWithErrors(const std::string msg, int errorCode, cta::log::LogContext & lc){
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportEndofSessionWithErrors");
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::synchronousReportEndWithErrors(), reporting asynchronously session complete.");
  m_continue=false;
  m_archiveMount->complete();
  if(m_errorHappened) {
    cta::log::ScopedParamContainer sp(lc);
    sp.add("errorMessage", msg)
      .add("errorCode", errorCode);
    lc.log(cta::log::INFO,"Reported end of session with error to client after sending file errors");
  } else{
    const std::string& msg ="Reported end of session with error to client";
    // As a measure of safety we censor any session error which is not ENOSPC into
    // Meaningless 666 (used to be SEINTERNAL in CASTOR). ENOSPC is the only one interpreted by the tape gateway.
    if (ENOSPC != errorCode) {
      errorCode = 666;
    }
    lc.log(cta::log::INFO,msg);
  }
  if(m_watchdog) {
    m_watchdog->addParameter(cta::log::Param("status",
      ENOSPC == errorCode?"success":"failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500*1000);
  }
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSuccessful::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_successfulArchiveJobs.push(std::move(m_successfulArchiveJob));
}

//------------------------------------------------------------------------------
//reportDriveStatus
//------------------------------------------------------------------------------
void MigrationReportPacker::reportDriveStatus(cta::common::dataStructures::DriveStatus status, cta::log::LogContext & lc) {
  cta::log::ScopedParamContainer params(lc);
  params.add("type", "ReportDriveStatus")
        .add("Status", cta::common::dataStructures::toString(status));
  lc.log(cta::log::DEBUG, "In MigrationReportPacker::reportDriveStatus(), pushing a report.");
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportDriveStatus(status));
}

//------------------------------------------------------------------------------
//ReportDriveStatus::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportDriveStatus::execute(MigrationReportPacker& parent){
  cta::log::ScopedParamContainer params(parent.m_lc);
  params.add("status", cta::common::dataStructures::toString(m_status));
  parent.m_lc.log(cta::log::DEBUG, "In MigrationReportPacker::ReportDriveStatus::execute(): reporting drive status.");
  parent.m_archiveMount->setDriveStatus(m_status);
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
      reportPacker.m_lc.log(cta::log::INFO,"Received a flush report from tape, but had no file to report to client. Doing nothing.");
      return;
    }
    reportPacker.m_archiveMount->reportJobsBatchWritten(reportPacker.m_successfulArchiveJobs, reportPacker.m_lc);
  } else {
    // This is an abnormal situation: we should never flush after an error!
    reportPacker.m_lc.log(cta::log::ALERT,"Received a flush after an error: sending file errors to client");
  }
}

//------------------------------------------------------------------------------
//reportTapeFull()::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportTapeFull::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_archiveMount->setTapeFull();
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_continue=false;
  reportPacker.m_lc.log(cta::log::DEBUG, "In MigrationReportPacker::ReportEndofSession::execute(): reporting session complete.");
  reportPacker.m_archiveMount->complete();
  if(!reportPacker.m_errorHappened){
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    reportPacker.m_lc.log(cta::log::INFO,"Reported end of session to client");
    if(reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(cta::log::Param("status","success"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
  else {
    // We have some errors
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", "Previous file errors");
    reportPacker.m_lc.log(cta::log::ERR,"Reported end of session with error to client due to previous file errors");
    if(reportPacker.m_watchdog) {
      reportPacker.m_watchdog->addParameter(cta::log::Param("status","failure"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_continue=false;
  reportPacker.m_lc.log(cta::log::DEBUG, "In MigrationReportPacker::ReportEndofSessionWithErrors::execute(): reporting session complete.");
  reportPacker.m_archiveMount->complete();
  if(reportPacker.m_errorHappened) {
    cta::log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", m_message)
      .add("errorCode", m_errorCode);
    reportPacker.m_lc.log(cta::log::INFO,"Reported end of session with error to client after sending file errors");
  } else{
    const std::string& msg ="Reported end of session with error to client";
    // As a measure of safety we censor any session error which is not ENOSPC into
    // SEINTERNAL. ENOSPC is the only one interpreted by the tape gateway.
    if (ENOSPC != m_errorCode) {
      m_errorCode = 666;
    }
    reportPacker.m_lc.log(cta::log::INFO,msg);
  }
  if(reportPacker.m_watchdog) {
    reportPacker.m_watchdog->addParameter(cta::log::Param("status",
      ENOSPC == m_errorCode?"success":"failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500*1000);
  }
}
//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportError::execute(MigrationReportPacker& reportPacker){
  reportPacker.m_errorHappened=true;
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", m_ex.getMessageValue())
          .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,"In MigrationReportPacker::ReportError::execute(): failing archive job after exception.");
  }
  try {
    m_failedArchiveJob->failed(cta::exception::Exception(m_ex.getMessageValue()), reportPacker.m_lc);
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue())
          .add("fileId", m_failedArchiveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,"In MigrationReportPacker::ReportError::execute(): call to m_failedArchiveJob->failed() threw an exception.");
  }
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
  // Create our own log context for the new thread.
  cta::log::LogContext lc = m_parent.m_lc;
  lc.pushOrReplace(cta::log::Param("thread", "ReportPacker"));
  try{
    while(m_parent.m_continue) {
      std::unique_ptr<Report> rep (m_parent.m_fifo.pop());
      {
        cta::log::ScopedParamContainer params(lc);
        int demangleStatus;
        char * demangledReportType = abi::__cxa_demangle(typeid(*rep.get()).name(), nullptr, nullptr, &demangleStatus);
        if (!demangleStatus) {
          params.add("typeId", demangledReportType);
        } else {
          params.add("typeId", typeid(*rep.get()).name());
        }
        free(demangledReportType);
        lc.log(cta::log::DEBUG,"In MigrationReportPacker::WorkerThread::run(): Got a new report.");
      }
      try{
        rep->execute(m_parent);
      }
      catch(const cta::ArchiveMount::FailedMigrationRecallResult& e){
        //here we catch a failed report MigrationResult. We try to close and if that fails too
        //we end up in the catch below
        lc.log(cta::log::INFO,"Successfully closed client's session after the failed report MigrationResult");
        if (m_parent.m_watchdog) {
          m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
          m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
        }
        break;
      }
    }
  } catch(const cta::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMSG", e.getMessageValue());
    lc.log(cta::log::ERR, "Tried to report endOfSession or endofSessionWithErrors and got a CTA exception, cant do much more.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
    }
  } catch(const std::exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    cta::log::ScopedParamContainer params(lc);
    params.add("exceptionMSG", e.what());
    int demangleStatus;
    char * demangleExceptionType = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &demangleStatus);
    if (!demangleStatus) {
      params.add("exceptionType", demangleExceptionType);
    } else {
      params.add("exceptionType", typeid(e).name());
    }
    lc.log(cta::log::ERR, "Tried to report endOfSession or endofSessionWithErrors and got a standard exception, cant do much more.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
    }
  } catch(...){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    lc.log(cta::log::ERR, "Tried to report endOfSession or endofSessionWithErrors and got an unknown exception, cant do much more.");
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
    }
  }
  // Drain the FIFO if necessary. We know that m_continue will be 
  // set by ReportEndofSessionWithErrors or ReportEndofSession
  // TODO devise a more generic mechanism
  while(m_parent.m_fifo.size()) {
    std::unique_ptr<Report> rep (m_parent.m_fifo.pop());
    cta::log::ScopedParamContainer params(lc);
    int demangleStatus;
    char * demangledReportType = abi::__cxa_demangle(typeid(*rep.get()).name(), nullptr, nullptr, &demangleStatus);
    if (!demangleStatus) {
      params.add("typeId", demangledReportType);
    } else {
      params.add("typeId", typeid(*rep.get()).name());
    }
    free(demangledReportType);
    lc.log(cta::log::DEBUG,"In MigrationReportPacker::WorkerThread::run(): Draining leftover.");
  }
}

}}}}
