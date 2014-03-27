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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
 
using castor::log::LogContext;
using castor::log::Param;

RecallReportPacker::RecallReportPacker(client::ClientInterface & tg,unsigned int reportFilePeriod,log::LogContext lc):
ReportPackerInterface<detail::Recall>(tg,lc),
        m_workerThread(*this),m_reportFilePeriod(reportFilePeriod),m_errorHappened(false),m_continue(true){

}
RecallReportPacker::~RecallReportPacker(){

}
void RecallReportPacker::reportCompletedJob(const tapegateway::FileToRecallStruct& recalledFile){
  std::auto_ptr<Report> rep(new ReportSuccessful(recalledFile));
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
  
void RecallReportPacker::reportFailedJob(const tapegateway::FileToRecallStruct & recalledFile
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
  
  _this.m_listReports->addSuccessfulRecalls(successRecall.release());
}

void RecallReportPacker::flush(){
  unsigned int totalSize = m_listReports->failedRecalls().size() +
                       m_listReports->successfulRecalls().size();
  if(totalSize > 0){
    logReport(m_listReports->failedRecalls(),"A file failed to be recalled");
    logReport(m_listReports->successfulRecalls(),"A file was successfully recalled");
    
    client::ClientInterface::RequestReport chrono;
    m_client.reportRecallResults(*m_listReports,chrono);
    m_listReports.reset(new FileReportList);
  }
}

void RecallReportPacker::ReportEndofSession::execute(RecallReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
  if(!_this.m_errorHappened){
    _this.m_client.reportEndOfSession(chrono);
    _this.m_lc.log(LOG_INFO,"Nominal RecallReportPacker::EndofSession has been reported");
  }
  else {
     const std::string& msg ="RecallReportPacker::EndofSession has been reported  but an error happened somewhere in the process";
     _this.m_lc.log(LOG_ERR,msg);
     _this.m_client.reportEndOfSessionWithError(msg,SEINTERNAL,chrono);
     //throw castor::exception::Exception(msg);
  }
  _this.m_continue=false;
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
   //throw castor::exception::Exception(msg);
  }
  _this.m_continue=false;
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
  while(m_parent.m_continue) {    
    std::auto_ptr<Report> rep (m_parent.m_fifo.pop());    
    unsigned int totalSize = m_parent.m_listReports->failedRecalls().size() +
                             m_parent.m_listReports->successfulRecalls().size();
    
    if(totalSize >= m_parent.m_reportFilePeriod || true==rep->goingToEnd())
    {
      m_parent.flush();
    }

    rep->execute(m_parent);
  }
}
}}}}
