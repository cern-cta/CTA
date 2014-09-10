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

#include <memory>

#include "castor/tape/tapeserver/daemon/MigrationReportPacker.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tapegateway/FileMigratedNotificationStruct.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
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
MigrationReportPacker::MigrationReportPacker(client::ClientInterface & tg,castor::log::LogContext lc):
ReportPackerInterface<detail::Migration>(tg,lc),
m_workerThread(*this),m_errorHappened(false),m_continue(true) {
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
  std::auto_ptr<Report> rep(new ReportSuccessful(migratedFile,checksum,blockId));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFailedJob
//------------------------------------------------------------------------------ 
void MigrationReportPacker::reportFailedJob(const tapegateway::FileToMigrateStruct& migratedFile,
        const std::string& msg,int error_code){
  std::auto_ptr<Report> rep(new ReportError(migratedFile,msg,error_code));
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
//------------------------------------------------------------------------------
//reportFlush
//------------------------------------------------------------------------------
void MigrationReportPacker::reportFlush(drives::compressionStats compressStats){
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
//ReportSuccessful::reportStuckOn
//------------------------------------------------------------------------------
void MigrationReportPacker::reportStuckOn(FileStruct& file){
}
//------------------------------------------------------------------------------
//ReportSuccessful::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSuccessful::execute(MigrationReportPacker& _this){
  std::auto_ptr<tapegateway::FileMigratedNotificationStruct> successMigration(new tapegateway::FileMigratedNotificationStruct);
  successMigration->setFseq(m_migratedFile.fseq());
  successMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
  successMigration->setId(m_migratedFile.id());
  successMigration->setNshost(m_migratedFile.nshost());
  successMigration->setFileid(m_migratedFile.fileid());
  successMigration->setBlockId0((m_blockId >> 24) & 0xFF);
  successMigration->setBlockId1((m_blockId >> 16) & 0xFF);
  successMigration->setBlockId2((m_blockId >>  8) & 0xFF);
  successMigration->setBlockId3((m_blockId >>  0) & 0xFF);
  //WARNING; Ad-hoc name of the ChecksumName !!");
  successMigration->setChecksumName("adler32");
  successMigration->setChecksum(m_checksum);

  successMigration->setFileSize(m_migratedFile.fileSize());

//  successMigration->setBlockId0(m_migratedFile.BlockId0());
//  successMigration->setBlockId1();
//  successMigration->setBlockId2();
//  successMigration->setBlockId3();

  _this.m_listReports->addSuccessfulMigrations(successMigration.release());
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
void MigrationReportPacker::Report::reportFileErrors(MigrationReportPacker& _this)
{
  // Some errors still have to be transmitted to the client, but not the
  // successful writes which were not validated by a flush (they will be
  // discarded)
  if(_this.m_listReports->failedMigrations().size()) {
    tapeserver::client::ClientInterface::RequestReport chrono;
    // First, cleanup the report of existing successes
    for (size_t i=0; i<_this.m_listReports->successfulMigrations().size(); i++) {
      delete _this.m_listReports->successfulMigrations()[i];
    }
    _this.m_listReports->successfulMigrations().resize(0);
    // Report those errors to the client
    _this.logReportWithError(_this.m_listReports->failedMigrations(),
      "Will report failed file to the client before end of session"); 
    _this.m_client.reportMigrationResults(*(_this.m_listReports),chrono);
    log::ScopedParamContainer sp(_this.m_lc);
    sp.add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
    _this.m_lc.log(LOG_INFO, "Reported failed file(s) to the client before end of session");
    // Reset the report lists.
    _this.m_listReports.reset(new tapegateway::FileMigrationReportList);
  }
}
//------------------------------------------------------------------------------
//ReportFlush::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& _this){

  if(!_this.m_errorHappened){
    tapeserver::client::ClientInterface::RequestReport chrono;
    try{

      computeCompressedSize(_this.m_listReports->successfulMigrations().begin(),
                            _this.m_listReports->successfulMigrations().end());
      _this.m_client.reportMigrationResults(*(_this.m_listReports),chrono);   
      _this.logReport(_this.m_listReports->successfulMigrations(),"A file was successfully written on the tape"); 
      log::ScopedParamContainer container(_this.m_lc);
      container.add("batch size",_this.m_listReports->successfulMigrations().size())
               .add("compressed",m_compressStats.toTape)
               .add("Non compressed",m_compressStats.fromHost);
      _this.m_lc.log(LOG_INFO,"Reported to the client that a batch of file was written on tape");
    }
    catch(const castor::exception::Exception& e){
      LogContext::ScopedParam sp[]={
        LogContext::ScopedParam(_this.m_lc, Param("exceptionCode",e.code())),
        LogContext::ScopedParam(_this.m_lc, Param("exceptionMessageValue", e.getMessageValue())),
        LogContext::ScopedParam(_this.m_lc, Param("exceptionWhat",e.what()))
      };
      tape::utils::suppresUnusedVariable(sp);
      
      const std::string msg_error="An exception was caught trying to call reportMigrationResults";
      _this.m_lc.log(LOG_ERR,msg_error);
      throw failedMigrationRecallResult(msg_error);
    }
  } else {
    // This is an abnormal situation: we should never flush after an error!
    _this.m_lc.log(LOG_ALERT,"Received a flush after an error: sending file errors to client");
    reportFileErrors(_this);
  }
  //reset (ie delete and replace) the current m_listReports.
  //Thus all current reports are deleted otherwise they would have been sent again at the next flush
  _this.m_listReports.reset(new tapegateway::FileMigrationReportList);
}

//------------------------------------------------------------------------------
//ReportEndofSession::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
  if(!_this.m_errorHappened){
    _this.m_client.reportEndOfSession(chrono);
    log::ScopedParamContainer sp(_this.m_lc);
    sp.add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
    _this.m_lc.log(LOG_INFO,"Reported end of session to client");
  }
  else {
    reportFileErrors(_this);
    // We have some errors: report end of session as such to the client
    _this.m_client.reportEndOfSessionWithError("Previous file errors",SEINTERNAL,chrono); 
    log::ScopedParamContainer sp(_this.m_lc);
    sp.add("errorMessage", "Previous file errors")
      .add("errorCode", SEINTERNAL)
      .add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
    _this.m_lc.log(LOG_ERR,"Reported end of session with error to client due to previous file errors");
  }
  _this.m_continue=false;
}
//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
  
  if(_this.m_errorHappened) {
    reportFileErrors(_this);
    _this.m_client.reportEndOfSessionWithError(m_message,m_errorCode,chrono); 
    log::ScopedParamContainer sp(_this.m_lc);
    sp.add("errorMessage", m_message)
      .add("errorCode", m_errorCode)
      .add("connectDuration", chrono.connectDuration)
      .add("sendRecvDuration", chrono.sendRecvDuration)
      .add("transactionId", chrono.transactionId);
    _this.m_lc.log(LOG_INFO,"Reported end of session with error to client after sending file errors");
  } else{
    const std::string& msg ="Reported end of session with error to client";
    // As a measure of safety we censor any session error which is not ENOSPC into
    // SEINTERNAL. ENOSPC is the only one interpreted by the tape gateway.
    if (ENOSPC != m_errorCode) {
      m_errorCode = SEINTERNAL;
    }
    _this.m_client.reportEndOfSessionWithError(msg,m_errorCode,chrono); 
    _this.m_lc.log(LOG_INFO,msg);
  }
  _this.m_continue=false;
}
//------------------------------------------------------------------------------
//ReportError::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportError::execute(MigrationReportPacker& _this){
   
  std::auto_ptr<tapegateway::FileErrorReportStruct> failedMigration(new tapegateway::FileErrorReportStruct);
  //failedMigration->setFileMigrationReportList(_this.m_listReports.get());
  failedMigration->setErrorCode(m_error_code);
  failedMigration->setErrorMessage(m_error_msg);
  failedMigration->setFseq(m_migratedFile.fseq());
  failedMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
  failedMigration->setId(m_migratedFile.id());
  failedMigration->setNshost(m_migratedFile.nshost());
  
  _this.m_listReports->addFailedMigrations(failedMigration.release());
  _this.m_errorHappened=true;
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
      std::auto_ptr<Report> rep (m_parent.m_fifo.pop());
      try{
        rep->execute(m_parent);
      }
      catch(const failedMigrationRecallResult& e){
        //here we catch a failed report MigrationResult. We try to close and it that fails too
        //we end up in the catch below
        m_parent.m_client.reportEndOfSessionWithError(e.getMessageValue(),SEINTERNAL,chrono);
        m_parent.logRequestReport(chrono,"Successfully closed client's session after the failed report MigrationResult");
        break;
      }
    }
  }
  catch(const castor::exception::Exception& e){
    //we get there because to tried to close the connection and it failed
    //either from the catch a few lines above or directly from rep->execute
    m_parent.logRequestReport(chrono,"tried to report endOfSession(WithError) and got an exception, cant do much more",LOG_ERR);
  }
}

}}}}
