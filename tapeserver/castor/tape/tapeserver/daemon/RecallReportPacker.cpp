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

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "common/log/Logger.hpp"
#include "common/utils/utils.hpp"
#include "objectstore/Backend.hpp"

#include <signal.h>
#include <iostream>
#include <cxxabi.h>

namespace{
  struct failedReportRecallResult : public cta::exception::Exception{
    failedReportRecallResult(const std::string& s): Exception(s){}
  };
}

using cta::log::LogContext;
using cta::log::Param;

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
//------------------------------------------------------------------------------
//Constructor
//------------------------------------------------------------------------------
RecallReportPacker::RecallReportPacker(cta::RetrieveMount *retrieveMount, cta::log::LogContext lc):
  ReportPackerInterface<detail::Recall>(lc),
  m_workerThread(*this), m_errorHappened(false), m_retrieveMount(retrieveMount),
  m_tapeThreadComplete(false), m_diskThreadComplete(false)
{
  
}
//------------------------------------------------------------------------------
//Destructor
//------------------------------------------------------------------------------
RecallReportPacker::~RecallReportPacker(){
  cta::threading::MutexLocker ml(m_producterProtection);
}
//------------------------------------------------------------------------------
//reportCompletedJob
//------------------------------------------------------------------------------
void RecallReportPacker::reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob){
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulRetrieveJob)));
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------  
void RecallReportPacker::reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob, const cta::exception::Exception & ex){
  std::string failureLog = cta::utils::getCurrentLocalTime() + " " + cta::utils::getShortHostname() +
      " " + ex.getMessageValue();
  std::unique_ptr<Report> rep(new ReportError(std::move(failedRetrieveJob), failureLog));
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------
void RecallReportPacker::reportEndOfSession(){
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportEndofSession());
}

//------------------------------------------------------------------------------
//reportDriveStatus
//------------------------------------------------------------------------------
void RecallReportPacker::reportDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason) {
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportDriveStatus(status,reason));
}

  
//------------------------------------------------------------------------------
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------
void RecallReportPacker::reportEndOfSessionWithErrors(const std::string msg,int error_code){
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
}

  
//------------------------------------------------------------------------------
//reportTestGoingToEnd
//------------------------------------------------------------------------------
void RecallReportPacker::reportTestGoingToEnd(){
  cta::threading::MutexLocker ml(m_producterProtection);
  m_fifo.push(new ReportTestGoingToEnd());
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportSuccessful::execute(RecallReportPacker& parent){
  try{
    m_successfulRetrieveJob->asyncSetSuccessful();
    parent.m_successfulRetrieveJobs.push(std::move(m_successfulRetrieveJob));
  } catch (const cta::objectstore::Backend::NoSuchObject &ex){
    cta::log::ScopedParamContainer params(parent.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue())
          .add("fileId", m_successfulRetrieveJob->archiveFile.archiveFileID);
    parent.m_lc.log(cta::log::WARNING,"In RecallReportPacker::ReportSuccessful::execute(): call to m_successfulRetrieveJob->asyncSetSuccessful() failed, job does not exist in the objectstore.");
  }
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSession::execute(RecallReportPacker& parent){
  parent.setDiskDone();
  if(!parent.errorHappened()){
    parent.m_lc.log(cta::log::INFO,"Nominal RecallReportPacker::EndofSession has been reported");
    if (parent.m_watchdog) {
      parent.m_watchdog->addParameter(cta::log::Param("status","success"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
  else {
    const std::string& msg ="RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process";
    parent.m_lc.log(cta::log::ERR,msg);
    if (parent.m_watchdog) {
      parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
}

//------------------------------------------------------------------------------
//ReportEndofSession::goingToEnd
//------------------------------------------------------------------------------
bool RecallReportPacker::ReportEndofSession::goingToEnd() {
  return true;
}

//------------------------------------------------------------------------------
//ReportDriveStatus::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportDriveStatus::execute(RecallReportPacker& parent){
  parent.m_retrieveMount->setDriveStatus(m_status,m_reason);
  if(m_status==cta::common::dataStructures::DriveStatus::Unmounting) {
    parent.setTapeDone();
    parent.setTapeComplete();
  }
}

//------------------------------------------------------------------------------
//ReportDriveStatus::goingToEnd
//------------------------------------------------------------------------------
bool RecallReportPacker::ReportDriveStatus::goingToEnd() {
  return false;
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSessionWithErrors::execute(RecallReportPacker& parent){
  parent.setDiskDone();
  if(parent.m_errorHappened) {
    LogContext::ScopedParam(parent.m_lc,Param("errorCode",m_error_code));
    parent.m_lc.log(cta::log::ERR,m_message);
  }
  else{
    const std::string& msg ="RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process";
    parent.m_lc.log(cta::log::ERR,msg);
  }
  if (parent.m_watchdog) {
    parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500*1000);
  }
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::goingToEnd
//------------------------------------------------------------------------------
bool RecallReportPacker::ReportEndofSessionWithErrors::goingToEnd() {
  return true;
}

//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportError::execute(RecallReportPacker& reportPacker){
  reportPacker.m_errorHappened=true;
  {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("failureLog", m_failureLog)
          .add("fileId", m_failedRetrieveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,"In RecallReportPacker::ReportError::execute(): failing retrieve job after exception.");
  }
  try {
    m_failedRetrieveJob->transferFailed(m_failureLog, reportPacker.m_lc);
  } catch (const cta::objectstore::Backend::NoSuchObject &ex){
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue())
          .add("fileId", m_failedRetrieveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::WARNING,"In RecallReportPacker::ReportError::execute(): call to m_failedRetrieveJob->failed() , job does not exist in the objectstore.");
  } catch (cta::exception::Exception & ex) {
    cta::log::ScopedParamContainer params(reportPacker.m_lc);
    params.add("ExceptionMSG", ex.getMessageValue())
          .add("fileId", m_failedRetrieveJob->archiveFile.archiveFileID);
    reportPacker.m_lc.log(cta::log::ERR,"In RecallReportPacker::ReportError::execute(): call to m_failedRetrieveJob->failed() threw an exception.");
    reportPacker.m_lc.logBacktrace(cta::log::ERR, ex.backtrace());
  }
}

//------------------------------------------------------------------------------
//WorkerThread::WorkerThread
//------------------------------------------------------------------------------
RecallReportPacker::WorkerThread::WorkerThread(RecallReportPacker& parent):
m_parent(parent) {
}
//------------------------------------------------------------------------------
//WorkerThread::run
//------------------------------------------------------------------------------
void RecallReportPacker::WorkerThread::run(){
  m_parent.m_lc.pushOrReplace(Param("thread", "RecallReportPacker"));
  m_parent.m_lc.log(cta::log::DEBUG, "Starting RecallReportPacker thread");
  bool endFound = false;
  
  std::list <std::unique_ptr<Report>> reportedSuccessfully;
  cta::utils::Timer t;
  while(1) {
    std::string debugType;
    std::unique_ptr<Report> rep(m_parent.m_fifo.pop());
    {
      cta::log::ScopedParamContainer spc(m_parent.m_lc);
      int demangleStatus;
      char * demangledReportType = abi::__cxa_demangle(typeid(*rep.get()).name(), nullptr, nullptr, &demangleStatus);
      if (!demangleStatus) {
        spc.add("typeId", demangledReportType);
      } else {
        spc.add("typeId", typeid(*rep.get()).name());
      }
      free(demangledReportType);
      if (rep->goingToEnd())
        spc.add("goingToEnd", "true");
      m_parent.m_lc.log(cta::log::DEBUG, "Popping report");
    }
    // Record whether we found end before calling the potentially exception
    // throwing execute().)
    if (rep->goingToEnd())
      endFound=true;
    // We can afford to see any report to fail and keep passing the following ones
    // as opposed to migrations where one failure fails the session.
    try {
      rep->execute(m_parent);
      // This slightly hackish bit prevents too many calls to sub function and gettime()
      // m_parent.fullCheckAndFinishAsyncExecute will execute the shared half of the
      // request updates (individual, asynchronous is done in rep->execute(m_parent);
      if (typeid(*rep) == typeid(RecallReportPacker::ReportSuccessful) 
          && (m_parent.m_successfulRetrieveJobs.size() >= m_parent.RECALL_REPORT_PACKER_FLUSH_SIZE || t.secs() >= m_parent.RECALL_REPORT_PACKER_FLUSH_TIME )){
        m_parent.m_lc.log(cta::log::INFO,"m_parent.fullCheckAndFinishAsyncExecute()");
        m_parent.fullCheckAndFinishAsyncExecute();
        t.reset();
      }
    } catch(const cta::exception::Exception& e){
      //we get there because to tried to close the connection and it failed
      //either from the catch a few lines above or directly from rep->execute
      cta::log::ScopedParamContainer params(m_parent.m_lc);
      params.add("exceptionWhat", e.getMessageValue())
            .add("exceptionType", typeid(e).name());
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got a CTA exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      }
    } catch(const std::exception& e){
      //we get there because to tried to close the connection and it failed
      //either from the catch a few lines above or directly from rep->execute
      cta::log::ScopedParamContainer params(m_parent.m_lc);
      params.add("exceptionWhat", e.what())
            .add("exceptionType", typeid(e).name());
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got a standard exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      }
    } catch(...){
      //we get there because to tried to close the connection and it failed
      //either from the catch a few lines above or directly from rep->execute
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got an unknown exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      }
    }
    if (endFound) break;
  }
  
  // Make sure the last batch of reports got cleaned up. 
  try {
    m_parent.fullCheckAndFinishAsyncExecute();
    if(m_parent.isDiskDone()){
      //The m_parent.m_diskThreadComplete is set to true when a ReportEndOfSession or a ReportAndOfSessionWithError
      //has been put. It is only after the fullCheckandFinishAsyncExecute is finished that we can say to the mount that the disk thread is complete.
      m_parent.m_lc.log(cta::log::DEBUG, "In RecallReportPacker::WorkerThread::run(): all disk threads are finished, telling the mount that Disk threads are complete");
      m_parent.setDiskComplete();
    }
  } catch(const cta::exception::Exception& e){
      cta::log::ScopedParamContainer params(m_parent.m_lc);
      params.add("exceptionWhat", e.getMessageValue())
            .add("exceptionType", typeid(e).name());
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got a CTA exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      } 
  } catch(const std::exception& e){
      cta::log::ScopedParamContainer params(m_parent.m_lc);
      params.add("exceptionWhat", e.what())
            .add("exceptionType", typeid(e).name());
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got a standard exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      }
  } catch(...){
      m_parent.m_lc.log(cta::log::ERR, "Tried to report and got an unknown exception.");
      if (m_parent.m_watchdog) {
        m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
        m_parent.m_watchdog->addParameter(cta::log::Param("status","failure"));
      }
  } 
  
  // Drain the fifo in case we got an exception
  if (!endFound) {
    while (1) {
      std::unique_ptr<Report> report(m_parent.m_fifo.pop());
      if (report->goingToEnd())
        break;
    }
  }
  // Cross check that the queue is indeed empty.
  while (m_parent.m_fifo.size()) {
    // There is at least one extra report we missed.
    // The drive status reports are not a problem though.
    cta::log::ScopedParamContainer spc(m_parent.m_lc);
    std::unique_ptr<Report> missedReport(m_parent.m_fifo.pop());
    spc.add("ReportType", typeid(*missedReport).name());
    if (missedReport->goingToEnd())
      spc.add("goingToEnd", "true");
    if (typeid(*missedReport) != typeid(RecallReportPacker::ReportDriveStatus))
      m_parent.m_lc.log(cta::log::ERR, "Popping missed report (memory leak)");
  }
  m_parent.m_lc.log(cta::log::DEBUG, "Finishing RecallReportPacker thread");
}

//------------------------------------------------------------------------------
//errorHappened()
//------------------------------------------------------------------------------
bool RecallReportPacker::errorHappened() {
  return m_errorHappened || (m_watchdog && m_watchdog->errorHappened());
}

//------------------------------------------------------------------------------
//fullCheckAndFinishAsyncExecute()
//------------------------------------------------------------------------------
void RecallReportPacker::fullCheckAndFinishAsyncExecute() {
  m_retrieveMount->flushAsyncSuccessReports(m_successfulRetrieveJobs, m_lc);
}

//------------------------------------------------------------------------------
//reportTapeDone()
//------------------------------------------------------------------------------
void RecallReportPacker::setTapeDone() {
  cta::threading::MutexLocker mutexLocker(m_mutex);
  m_tapeThreadComplete = true;
}

void RecallReportPacker::setTapeComplete(){
  cta::threading::MutexLocker mutexLocker(m_mutex);
  m_retrieveMount->tapeComplete();
}

void RecallReportPacker::setDiskComplete(){
  cta::threading::MutexLocker mutexLocker(m_mutex);
  m_retrieveMount->diskComplete();
}

bool RecallReportPacker::isDiskDone(){
  cta::threading::MutexLocker mutexLocker(m_mutex);
  return m_diskThreadComplete;
}

//------------------------------------------------------------------------------
//reportDiskDone()
//------------------------------------------------------------------------------
void RecallReportPacker::setDiskDone() {
  cta::threading::MutexLocker mutexLocker(m_mutex);
  m_diskThreadComplete = true;
}

//------------------------------------------------------------------------------
//reportDiskDone()
//------------------------------------------------------------------------------
bool RecallReportPacker::allThreadsDone() {
  return m_tapeThreadComplete && m_diskThreadComplete;
}

}}}}
