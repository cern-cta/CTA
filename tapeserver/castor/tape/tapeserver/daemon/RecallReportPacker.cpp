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

#include "castor/tape/tapeserver/daemon/RecallReportPacker.hpp"
#include "castor/tape/tapeserver/daemon/TaskWatchDog.hpp"
#include "castor/log/Logger.hpp"

#include <signal.h>
#include <iostream>

namespace{
  struct failedReportRecallResult : public castor::exception::Exception{
    failedReportRecallResult(const std::string& s): Exception(s){}
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
RecallReportPacker::RecallReportPacker(cta::RetrieveMount *retrieveMount, log::LogContext lc):
  ReportPackerInterface<detail::Recall>(lc),
  m_workerThread(*this), m_errorHappened(false), m_retrieveMount(retrieveMount),
  m_tapeThreadComplete(false), m_diskThreadComplete(false)
{

}
//------------------------------------------------------------------------------
//Destructor
//------------------------------------------------------------------------------
RecallReportPacker::~RecallReportPacker(){
  castor::server::MutexLocker ml(&m_producterProtection);
}
//------------------------------------------------------------------------------
//reportCompletedJob
//------------------------------------------------------------------------------
void RecallReportPacker::reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob){
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulRetrieveJob)));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------  
void RecallReportPacker::reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob){
  std::unique_ptr<Report> rep(new ReportError(std::move(failedRetrieveJob)));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportEndOfSession
//------------------------------------------------------------------------------
void RecallReportPacker::reportEndOfSession(){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSession());
}

//------------------------------------------------------------------------------
//reportDriveStatus
//------------------------------------------------------------------------------
void RecallReportPacker::reportDriveStatus(cta::common::DriveStatus status) {
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportDriveStatus(status));
}

  
//------------------------------------------------------------------------------
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------
void RecallReportPacker::reportEndOfSessionWithErrors(const std::string msg,int error_code){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
}

  
//------------------------------------------------------------------------------
//reportTestGoingToEnd
//------------------------------------------------------------------------------
void RecallReportPacker::reportTestGoingToEnd(){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportTestGoingToEnd());
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportSuccessful::execute(RecallReportPacker& parent){
  m_successfulRetrieveJob->complete();
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSession::execute(RecallReportPacker& parent){
  if(!parent.errorHappened()){
    parent.m_lc.log(LOG_INFO,"Nominal RecallReportPacker::EndofSession has been reported");
    if (parent.m_watchdog) {
      parent.m_watchdog->addParameter(log::Param("status","success"));
      // We have a race condition here between the processing of this message by
      // the initial process and the printing of the end-of-session log, triggered
      // by the end our process. To delay the latter, we sleep half a second here.
      usleep(500*1000);
    }
  }
  else {
    const std::string& msg ="RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process";
    parent.m_lc.log(LOG_ERR,msg);
    if (parent.m_watchdog) {
      parent.m_watchdog->addParameter(log::Param("status","failure"));
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
bool RecallReportPacker::ReportEndofSession::goingToEnd(RecallReportPacker& packer) {
  return false;
}

//------------------------------------------------------------------------------
//ReportDriveStatus::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportDriveStatus::execute(RecallReportPacker& parent){
  parent.m_retrieveMount->setDriveStatus(m_status);
  if(m_status==cta::common::DriveStatus::Unmounting) {
    parent.m_retrieveMount->diskComplete();
    parent.m_retrieveMount->tapeComplete();
  }
}

//------------------------------------------------------------------------------
//ReportDriveStatus::goingToEnd
//------------------------------------------------------------------------------
bool RecallReportPacker::ReportDriveStatus::goingToEnd(RecallReportPacker& packer) {
  if(m_status==cta::common::DriveStatus::Unmounting) return true;
  return false;
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSessionWithErrors::execute(RecallReportPacker& parent){
  if(parent.m_errorHappened) {
    LogContext::ScopedParam(parent.m_lc,Param("errorCode",m_error_code));
    parent.m_lc.log(LOG_ERR,m_message);
  }
  else{
    const std::string& msg ="RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process";
    parent.m_lc.log(LOG_ERR,msg);
  }
  if (parent.m_watchdog) {
    parent.m_watchdog->addParameter(log::Param("status","failure"));
    // We have a race condition here between the processing of this message by
    // the initial process and the printing of the end-of-session log, triggered
    // by the end our process. To delay the latter, we sleep half a second here.
    usleep(500*1000);
  }
}

//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::goingToEnd
//------------------------------------------------------------------------------
bool RecallReportPacker::ReportEndofSessionWithErrors::goingToEnd(RecallReportPacker& packer) {
  return false;
}

//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportError::execute(RecallReportPacker& parent){
  parent.m_errorHappened=true;
  parent.m_lc.log(LOG_ERR,m_failedRetrieveJob->failureMessage);
  m_failedRetrieveJob->failed();
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
  m_parent.m_lc.log(LOG_DEBUG, "Starting RecallReportPacker thread");
  bool endFound = false;
  try{
    while(1) {    
      std::unique_ptr<Report> rep(m_parent.m_fifo.pop());
      rep->execute(m_parent);

      if(rep->goingToEnd(m_parent)) {
        endFound = true;
        break;
      }
    }
  } catch(const castor::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report and got a castor exception, cant do much more. The exception is the following: " << e.getMessageValue();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(const cta::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report and got a CTA exception, cant do much more. The exception is the following: " << e.getMessageValue();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(const std::exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report and got a standard exception, cant do much more. The exception is the following: " << e.what();
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  } catch(...){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    std::stringstream ssEx;
    ssEx << "Tried to report and got an unknown exception, cant do much more.";
    m_parent.m_lc.log(LOG_ERR, ssEx.str());
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  }
  // Drain the fifo in case we got an exception
  if (!endFound) {
    while (1) {
      std::unique_ptr<Report> report(m_parent.m_fifo.pop());
      if (report->goingToEnd(m_parent))
        break;
    }
  }
  m_parent.m_lc.log(LOG_DEBUG, "Finishing RecallReportPacker thread");
}

//------------------------------------------------------------------------------
//errorHappened()
//------------------------------------------------------------------------------
bool RecallReportPacker::errorHappened() {
  return m_errorHappened || (m_watchdog && m_watchdog->errorHappened());
}

//------------------------------------------------------------------------------
//reportTapeDone()
//------------------------------------------------------------------------------
void RecallReportPacker::setTapeDone() {
  m_tapeThreadComplete = true;
}

//------------------------------------------------------------------------------
//reportDiskDone()
//------------------------------------------------------------------------------
void RecallReportPacker::setDiskDone() {
  m_diskThreadComplete = true;
}

//------------------------------------------------------------------------------
//reportDiskDone()
//------------------------------------------------------------------------------
bool RecallReportPacker::allThreadsDone() {
  return m_tapeThreadComplete && m_diskThreadComplete;
}

}}}}
