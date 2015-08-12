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
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
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
  castor::log::LogContext lc):
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
const tapegateway::FileToMigrateStruct& migratedFile,u_int32_t checksum,
    u_int32_t blockId) {
  std::unique_ptr<Report> rep(new ReportSuccessful(migratedFile,checksum,blockId));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportFailedJob(const tapegateway::FileToMigrateStruct& migratedFile,
        const std::string& msg,int error_code){
  std::unique_ptr<Report> rep(new ReportError(migratedFile,msg,error_code));
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
//  std::unique_ptr<tapegateway::FileMigratedNotificationStruct> successMigration(new tapegateway::FileMigratedNotificationStruct);
//  successMigration->setFseq(m_migratedFile.fseq());
//  successMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
//  successMigration->setId(m_migratedFile.id());
//  successMigration->setNshost(m_migratedFile.nshost());
//  successMigration->setFileid(m_migratedFile.fileid());
//  successMigration->setBlockId0((m_blockId >> 24) & 0xFF);
//  successMigration->setBlockId1((m_blockId >> 16) & 0xFF);
//  successMigration->setBlockId2((m_blockId >>  8) & 0xFF);
//  successMigration->setBlockId3((m_blockId >>  0) & 0xFF);
//  //WARNING; Ad-hoc name of the ChecksumName !!");
//  successMigration->setChecksumName("adler32");
//  successMigration->setChecksum(m_checksum);
//
//  successMigration->setFileSize(m_migratedFile.fileSize());
//
////  successMigration->setBlockId0(m_migratedFile.BlockId0());
////  successMigration->setBlockId1();
////  successMigration->setBlockId2();
////  successMigration->setBlockId3();
//
//  reportPacker.m_listReports->addSuccessfulMigrations(successMigration.release());
  reportPacker.m_successfulArchiveJobs.push(std::move(m_successfulArchiveJob));
}
//------------------------------------------------------------------------------
//ReportFlush::computeCompressedSize
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::computeCompressedSize(
std::vector<tapegateway::FileMigratedNotificationStruct*>::iterator beg,
std::vector<tapegateway::FileMigratedNotificationStruct*>::iterator end
)
{
  //lets pray for C++11 lamba and std accumulate
  uint64_t rawSize = 0;
  for(std::vector<tapegateway::FileMigratedNotificationStruct*>::iterator  it = beg;
          it != end ;++it){
            rawSize+=(*it)->fileSize();
  }

  uint64_t nbByteWritenWithCompression = m_compressStats.toTape;  
  //we dont want compressionRatio to be equal to zero not to have a division by zero
  double compressionRatio = nbByteWritenWithCompression>0 && rawSize >0 ? 
    1.0*nbByteWritenWithCompression/rawSize : 1.;
  
  for(std::vector<tapegateway::FileMigratedNotificationStruct*>::iterator  it = beg;
          it != end ;++it){
    const uint64_t compressedFileSize =
    static_cast<uint64_t>((*it)->fileSize() * compressionRatio);

    // The compressed file size should never be reported as being less than 1
    // byte
    uint64_t validCompressedFileSize = 0 < compressedFileSize ? compressedFileSize : 1;
    
    (*it)->setCompressedFileSize(validCompressedFileSize);
  }
}
//------------------------------------------------------------------------------
//Report::reportFileErrors
//------------------------------------------------------------------------------
//void MigrationReportPacker::Report::reportFileErrors(MigrationReportPacker& reportPacker)
//{
//  // Some errors still have to be transmitted to the client, but not the
//  // successful writes which were not validated by a flush (they will be
//  // discarded)
//  if(reportPacker.m_listReports->failedMigrations().size()) {
//    tapeserver::client::ClientInterface::RequestReport chrono;
//    // First, cleanup the report of existing successes
//    for (size_t i=0; i<reportPacker.m_listReports->successfulMigrations().size(); i++) {
//      delete reportPacker.m_listReports->successfulMigrations()[i];
//    }
//    reportPacker.m_listReports->successfulMigrations().resize(0);
//    // Report those errors to the client
//    reportPacker.logReportWithError(reportPacker.m_listReports->failedMigrations(),
//      "Will report failed file to the client before end of session"); 
//    reportPacker.m_client.reportMigrationResults(*(reportPacker.m_listReports),chrono);
//    log::ScopedParamContainer sp(reportPacker.m_lc);
//    sp.add("connectDuration", chrono.connectDuration)
//      .add("sendRecvDuration", chrono.sendRecvDuration)
//      .add("transactionId", chrono.transactionId);
//    reportPacker.m_lc.log(LOG_INFO, "Reported failed file(s) to the client before end of session");
//    // Reset the report lists.
//    reportPacker.m_listReports.reset(new tapegateway::FileMigrationReportList);
//  }
//}
//------------------------------------------------------------------------------
//ReportFlush::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& reportPacker){

  if(!reportPacker.m_errorHappened){
    tapeserver::client::ClientInterface::RequestReport chrono;
    // We can receive double flushes when the periodic flush happens
    // right before the end of session (which triggers also a flush)
    // We refrain from sending an empty report to the client in this case.
    if (0 == reportPacker.m_listReports->successfulMigrations().size() +
             reportPacker.m_listReports->failedMigrations().size()) {
      reportPacker.m_lc.log(LOG_INFO,"Received a flush report from tape, but had no file to report to client. Doing nothing.");
      return;
    }
    try{

      computeCompressedSize(reportPacker.m_listReports->successfulMigrations().begin(),
                            reportPacker.m_listReports->successfulMigrations().end());
      //reportPacker.m_client.reportMigrationResults(*(reportPacker.m_listReports),chrono);   
      while(reportPacker.m_successfulArchiveJobs.size()) {
        std::unique_ptr<cta::ArchiveJob> successfulArchiveJob = std::move(reportPacker.m_successfulArchiveJobs.front());
        reportPacker.m_successfulArchiveJobs.pop();
        successfulArchiveJob->complete();
      }
      reportPacker.logReport(reportPacker.m_listReports->successfulMigrations(),"A file was successfully written on the tape"); 
      log::ScopedParamContainer container(reportPacker.m_lc);
      container.add("batch size",reportPacker.m_listReports->successfulMigrations().size())
               .add("compressed",m_compressStats.toTape)
               .add("Non compressed",m_compressStats.fromHost);
      reportPacker.m_lc.log(LOG_INFO,"Reported to the client that a batch of file was written on tape");
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
//    reportFileErrors(reportPacker);
  }
  //reset (ie delete and replace) the current m_listReports.
  //Thus all current reports are deleted otherwise they would have been sent again at the next flush
  reportPacker.m_listReports.reset(new tapegateway::FileMigrationReportList);
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& reportPacker){
  client::ClientInterface::RequestReport chrono;
  if(!reportPacker.m_errorHappened){
    //reportPacker.m_client.reportEndOfSession(chrono);
    reportPacker.m_archiveMount->complete();
    log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
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
//    reportFileErrors(reportPacker);
    // We have some errors: report end of session as such to the client
    //reportPacker.m_client.reportEndOfSessionWithError("Previous file errors",SEINTERNAL,chrono); 
    reportPacker.m_archiveMount->failed(cta::exception::Exception("Previous file errors"));
    log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", "Previous file errors")
      .add("errorCode", SEINTERNAL)
      .add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
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
  client::ClientInterface::RequestReport chrono;
  
  if(reportPacker.m_errorHappened) {
//    reportFileErrors(reportPacker);
    //reportPacker.m_client.reportEndOfSessionWithError(m_message,m_errorCode,chrono);
    reportPacker.m_archiveMount->failed(cta::exception::Exception(m_message));
    log::ScopedParamContainer sp(reportPacker.m_lc);
    sp.add("errorMessage", m_message)
      .add("errorCode", m_errorCode)
      .add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
    reportPacker.m_lc.log(LOG_INFO,"Reported end of session with error to client after sending file errors");
  } else{
    const std::string& msg ="Reported end of session with error to client";
    // As a measure of safety we censor any session error which is not ENOSPC into
    // SEINTERNAL. ENOSPC is the only one interpreted by the tape gateway.
    if (ENOSPC != m_errorCode) {
      m_errorCode = SEINTERNAL;
    }
    //reportPacker.m_client.reportEndOfSessionWithError(msg,m_errorCode,chrono);
    reportPacker.m_archiveMount->failed(cta::exception::Exception(msg)); 
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
   
//  std::unique_ptr<tapegateway::FileErrorReportStruct> failedMigration(new tapegateway::FileErrorReportStruct);
//  //failedMigration->setFileMigrationReportList(reportPacker.m_listReports.get());
//  failedMigration->setErrorCode(m_error_code);
//  failedMigration->setErrorMessage(m_error_msg);
//  failedMigration->setFseq(m_migratedFile.fseq());
//  failedMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
//  failedMigration->setFileid(m_migratedFile.fileid());
//  failedMigration->setNshost(m_migratedFile.nshost());
//  failedMigration->setPositionCommandCode(m_migratedFile.positionCommandCode());
//  
//  reportPacker.m_listReports->addFailedMigrations(failedMigration.release());
  m_failedArchiveJob->failed();
  reportPacker.m_errorHappened=true;
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
  client::ClientInterface::RequestReport chrono;
  
  try{
    while(m_parent.m_continue) {
      std::unique_ptr<Report> rep (m_parent.m_fifo.pop());
      try{
        rep->execute(m_parent);
      }
      catch(const failedMigrationRecallResult& e){
        //here we catch a failed report MigrationResult. We try to close and it that fails too
        //we end up in the catch below
        //m_parent.m_client.reportEndOfSessionWithError(e.getMessageValue(),SEINTERNAL,chrono);
        m_parent.m_archiveMount->failed(e);
        m_parent.logRequestReport(chrono,"Successfully closed client's session after the failed report MigrationResult");
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
    m_parent.logRequestReport(chrono,"tried to report endOfSession(WithError) and got an exception, cant do much more",LOG_ERR);
    if (m_parent.m_watchdog) {
      m_parent.m_watchdog->addToErrorCount("Error_clientCommunication");
      m_parent.m_watchdog->addParameter(log::Param("status","failure"));
    }
  }
}

}}}}
