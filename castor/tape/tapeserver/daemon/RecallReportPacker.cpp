/******************************************************************************
 *                      RecallReportPacker.hpp
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
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/tape/tapegateway/FileRecalledNotificationStruct.hpp"
#include "castor/log/Logger.hpp"
#include "log.h"

namespace{
  struct failedReportRecallResult : public castor::tape::Exception{
    failedReportRecallResult(const std::string& s): Exception(s){}
  };
}

using castor::log::LogContext;
using castor::log::Param;

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
 
RecallReportPacker::RecallReportPacker(client::ClientInterface & tg,unsigned int reportFilePeriod,log::LogContext lc):
ReportPackerInterface<detail::Recall>(tg,lc),
        m_workerThread(*this),m_reportFilePeriod(reportFilePeriod),m_errorHappened(false){

}
RecallReportPacker::~RecallReportPacker(){

}
void RecallReportPacker::reportCompletedJob(const FileStruct& recalledFile,unsigned long checksum){
  std::auto_ptr<Report> rep(new ReportSuccessful(recalledFile,checksum));
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
  
void RecallReportPacker::reportFailedJob(const FileStruct & recalledFile
,const std::string& msg,int error_code){
  std::auto_ptr<Report> rep(new ReportError(recalledFile,msg,error_code));
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}

void RecallReportPacker::reportEndOfSession(){
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSession());
}
  

void RecallReportPacker::reportEndOfSessionWithErrors(const std::string msg,int error_code){
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
}
 
//------------------------------------------------------------------------------
void RecallReportPacker::ReportSuccessful::execute(RecallReportPacker& _this){
  std::auto_ptr<FileSuccessStruct> successRecall(new FileSuccessStruct);
  
  successRecall->setFseq(m_migratedFile.fseq());
  successRecall->setFileTransactionId(m_migratedFile.fileTransactionId());
  successRecall->setId(m_migratedFile.id());
  successRecall->setNshost(m_migratedFile.nshost());
  successRecall->setFileid(m_migratedFile.fileid());
  successRecall->setChecksum(m_checksum);
  
  //WARNING : ad hoc name of checksum algorithm
  successRecall->setChecksumName("adler32");
  _this.m_listReports->addSuccessfulRecalls(successRecall.release());
}

void RecallReportPacker::flush(){
  unsigned int totalSize = m_listReports->failedRecalls().size() +
                       m_listReports->successfulRecalls().size();
  if(totalSize > 0){
    
    client::ClientInterface::RequestReport chrono;
    try{
      m_client.reportRecallResults(*m_listReports,chrono);
      logRequestReport(chrono,"RecallReportList successfully transmitted to client (contents follow)");

      logReport(m_listReports->failedRecalls(),"A file failed to be recalled");
      logReport(m_listReports->successfulRecalls(),"A file was successfully recalled");
    }
   catch(const castor::tape::Exception& e){
    LogContext::ScopedParam s(m_lc, Param("exceptionCode",e.code()));
    LogContext::ScopedParam ss(m_lc, Param("exceptionMessageValue", e.getMessageValue()));
    LogContext::ScopedParam sss(m_lc, Param("exceptionWhat",e.what()));
    const std::string msg_error="An exception was caught trying to call reportRecallResults";
    m_lc.log(LOG_ERR,msg_error);
    throw failedReportRecallResult(msg_error);
    }
  }
    //delete the old pointer and replace it with the new one provided
    //that way, all the reports that have been send are deleted (by FileReportList's destructor)
    m_listReports.reset(new FileReportList);
}

void RecallReportPacker::ReportEndofSession::execute(RecallReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
    if(!_this.m_errorHappened){
      _this.m_client.reportEndOfSession(chrono);
      _this.logRequestReport(chrono,"Nominal RecallReportPacker::EndofSession has been reported",LOG_INFO);
    }
    else {
      const std::string& msg ="RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process";
      _this.m_lc.log(LOG_ERR,msg);
      _this.m_client.reportEndOfSessionWithError(msg,SEINTERNAL,chrono);
      _this.logRequestReport(chrono,"reporting EndOfSessionWithError done",LOG_ERR);
    }
}

void RecallReportPacker::ReportEndofSessionWithErrors::execute(RecallReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
  if(_this.m_errorHappened) {
  _this.m_client.reportEndOfSessionWithError(m_message,m_error_code,chrono); 
  LogContext::ScopedParam(_this.m_lc,Param("errorCode",m_error_code));
  _this.m_lc.log(LOG_ERR,m_message);
  }
  else{
   const std::string& msg ="RecallReportPacker::EndofSessionWithErrors has been reported  but NO error was detected during the process";
   _this.m_lc.log(LOG_ERR,msg);
   _this.m_client.reportEndOfSessionWithError(msg,SEINTERNAL,chrono); 
  }
}

void RecallReportPacker::ReportError::execute(RecallReportPacker& _this){
   
  std::auto_ptr<FileErrorStruct> failed(new FileErrorStruct);
  //failedMigration->setFileMigrationReportList(_this.m_listReports.get());
  failed->setErrorCode(m_error_code);
  failed->setErrorMessage(m_error_msg);
  failed->setFseq(m_migratedFile.fseq());
  failed->setFileTransactionId(m_migratedFile.fileTransactionId());
  failed->setId(m_migratedFile.id());
  failed->setNshost(m_migratedFile.nshost());
  
  _this.m_listReports->addFailedRecalls(failed.release());
  _this.m_errorHappened=true;
}
//------------------------------------------------------------------------------

RecallReportPacker::WorkerThread::WorkerThread(RecallReportPacker& parent):
m_parent(parent) {
}

void RecallReportPacker::WorkerThread::run(){
  m_parent.m_lc.pushOrReplace(Param("thread", "RecallReportPacker"));
  m_parent.m_lc.log(LOG_DEBUG, "Starting RecallReportPacker thread");
  client::ClientInterface::RequestReport chrono;
  try{
      while(1) {    
        std::auto_ptr<Report> rep (m_parent.m_fifo.pop());    
        
        unsigned int totalSize = m_parent.m_listReports->failedRecalls().size() +
                                 m_parent.m_listReports->successfulRecalls().size();
     
        if(totalSize >= m_parent.m_reportFilePeriod || rep->goingToEnd() )
        {
        
          try{
            m_parent.flush();
          }
          catch(const failedReportRecallResult& e){
            //got there because we failed tp report the recall results
            //we have to try to close the connection
            m_parent.m_client.reportEndOfSessionWithError(e.getMessageValue(),SEINTERNAL,chrono);
            m_parent.logRequestReport(chrono,"Successfully closed client's session after the failed report RecallResult");
            break;
          }
        }
        
        rep->execute(m_parent);
        if(rep->goingToEnd()) {
          break;
        }
    }
  }
  catch(const castor::tape::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    m_parent.logRequestReport(chrono,"tried to report endOfSession(WithError) and got an exception, cant do much more",LOG_ERR);
  }
  m_parent.m_lc.log(LOG_DEBUG, "Finishing RecallReportPacker thread");
}
}}}}
