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
const tapegateway::FileToMigrateStruct& migratedFile,unsigned long checksum) {
  std::auto_ptr<Report> rep(new ReportSuccessful(migratedFile,checksum));
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
void MigrationReportPacker::reportEndOfSessionWithErrors(std::string msg,int error_code){
  castor::server::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
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
  }
  else {
    const std::string& msg ="A flush on tape has been reported but a writing error happened before";
    _this.logReport(_this.m_listReports->failedMigrations(),msg); 
    //throw castor::exception::Exception(msg);
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
    _this.m_lc.log(LOG_INFO,"Nominal EndofSession has been reported without incident on tape-writing");
    _this.m_client.reportEndOfSession(chrono);
  }
  else {
     const std::string& msg ="Nominal EndofSession has been reported  but an writing error on the tape happened before";
     _this.m_client.reportEndOfSessionWithError(msg,SEINTERNAL,chrono); 
     _this.m_lc.log(LOG_ERR,msg);
  }
  _this.m_continue=false;
}
//------------------------------------------------------------------------------
//ReportEndofSessionWithErrors::execute
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& _this){
  client::ClientInterface::RequestReport chrono;
  
  if(_this.m_errorHappened) {
  _this.m_client.reportEndOfSessionWithError(m_message,m_error_code,chrono); 
  _this.m_lc.log(LOG_ERR,m_message);
  }
  else{
    const std::string& msg ="MigrationReportPacker::EndofSessionWithErrors has been reported  but NO writing error on the tape was detected";
   _this.m_client.reportEndOfSessionWithError(msg,SEINTERNAL,chrono); 
   _this.m_lc.log(LOG_ERR,msg);
   //throw castor::exception::Exception(msg);
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
