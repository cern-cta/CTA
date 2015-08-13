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
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/log/Logger.hpp"
#include "log.h"
#include "serrno.h"

#include <signal.h>

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
        m_workerThread(*this),m_errorHappened(false), m_retrieveMount(retrieveMount){

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
void RecallReportPacker::reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob,
  u_int32_t checksum, u_int64_t size){
  std::unique_ptr<Report> rep(new ReportSuccessful(std::move(successfulRetrieveJob),checksum,size));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------  
void RecallReportPacker::reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob
,const std::string& msg,int error_code){
  std::unique_ptr<Report> rep(new ReportError(std::move(failedRetrieveJob),msg,error_code));
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
//reportEndOfSessionWithErrors
//------------------------------------------------------------------------------
void RecallReportPacker::reportEndOfSessionWithErrors(const std::string msg,int error_code){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
}

//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportSuccessful::execute(RecallReportPacker& parent){
  m_successfulRetrieveJob->complete(m_checksum, m_size);
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSession::execute(RecallReportPacker& parent){
    if(!parent.errorHappened()){
      parent.m_retrieveMount->complete();
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
      parent.m_retrieveMount->failed(cta::exception::Exception(msg));
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
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportEndofSessionWithErrors::execute(RecallReportPacker& parent){
  if(parent.m_errorHappened) {
    parent.m_retrieveMount->failed(cta::exception::Exception(m_message));    
    LogContext::ScopedParam(parent.m_lc,Param("errorCode",m_error_code));
    parent.m_lc.log(LOG_ERR,m_message);
  }
  else{
   const std::string& msg ="RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process";
   parent.m_lc.log(LOG_ERR,msg);  
   parent.m_retrieveMount->failed(cta::exception::Exception(msg));    
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
//ReportError::execute
//------------------------------------------------------------------------------
void RecallReportPacker::ReportError::execute(RecallReportPacker& parent){
  parent.m_errorHappened=true;
  m_failedRetrieveJob->failed(cta::exception::Exception(m_error_msg));
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
  client::ClientInterface::RequestReport chrono;
  try{
    while(1) {    
      std::unique_ptr<Report> rep(m_parent.m_fifo.pop());    
      rep->execute(m_parent);

      if(rep->goingToEnd()) {
        break;
      }
    }
  }
  catch(const castor::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    m_parent.logRequestReport(chrono,"tried to report endOfSession(WithError) and got an exception, cant do much more",LOG_ERR);
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
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

}}}}
