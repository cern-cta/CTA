/******************************************************************************
 *                      MigrationReportPacker.cpp
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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
MigrationReportPacker::MigrationReportPacker(ClientInterface & tg):
m_workerThread(*this),m_client(tg) {
}

void MigrationReportPacker::reportCompletedJob(const tapegateway::FileToMigrateStruct& migratedFile) {
  std::auto_ptr<Report> rep(new ReportSuccessful(migratedFile));
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}

void MigrationReportPacker::reportFailedJob(const tapegateway::FileToMigrateStruct& migratedFile,
        const std::string& msg,int error_code){
  std::auto_ptr<Report> rep(new ReportError(migratedFile,msg,error_code));
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(rep.release());
}
void MigrationReportPacker::reportFlush() {
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportFlush());
}

void MigrationReportPacker::reportEndOfSession() {
    castor::tape::threading::MutexLocker ml(&m_producterProtection);
    m_fifo.push(new ReportEndofSession());
}
void MigrationReportPacker::reportEndOfSessionWithErrors(const std::string msg,int error_code){
  castor::tape::threading::MutexLocker ml(&m_producterProtection);
  m_fifo.push(new ReportEndofSessionWithErrors(msg,error_code));
}
//------------------------------------------------------------------------------
void MigrationReportPacker::ReportSuccessful::execute(MigrationReportPacker& _this){
  std::auto_ptr<tapegateway::FileMigratedNotificationStruct> successMigration(new tapegateway::FileMigratedNotificationStruct);
  
  successMigration->setFseq(m_migratedFile.fseq());
  successMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
  successMigration->setId(m_migratedFile.id());
  successMigration->setNshost(m_migratedFile.nshost());
  successMigration->setFileid(m_migratedFile.fileid());
  
  _this.m_listReports.addSuccessfulMigrations(successMigration.release());
}

void MigrationReportPacker::ReportFlush::execute(MigrationReportPacker& _this){
    tapeserver::daemon::ClientInterface::RequestReport chrono;
  _this.m_client.reportMigrationResults(_this.m_listReports,chrono);
}
  
void MigrationReportPacker::ReportEndofSession::execute(MigrationReportPacker& _this){
  tapeserver::daemon::ClientInterface::RequestReport chrono;
  _this.m_client.reportEndOfSession(chrono);
}

void MigrationReportPacker::ReportEndofSessionWithErrors::execute(MigrationReportPacker& _this){
  tapeserver::daemon::ClientInterface::RequestReport chrono;
  _this.m_listReports.successfulMigrations().clear();
  _this.m_client.reportEndOfSessionWithError(m_message,m_error_code,chrono);
}

void MigrationReportPacker::ReportError::execute(MigrationReportPacker& _this){
   
  std::auto_ptr<tapegateway::FileErrorReportStruct> failedMigration(new tapegateway::FileErrorReportStruct);

  failedMigration->setErrorCode(m_error_code);
  failedMigration->setErrorMessage(m_error_msg);
  failedMigration->setFseq(m_migratedFile.fseq());
  failedMigration->setFileTransactionId(m_migratedFile.fileTransactionId());
  failedMigration->setId(m_migratedFile.id());
  failedMigration->setNshost(m_migratedFile.nshost());
  
  _this.m_listReports.addFailedMigrations(failedMigration.release());
   _this.reportEndOfSessionWithErrors(m_error_msg,m_error_code);
}
//------------------------------------------------------------------------------
MigrationReportPacker::WorkerThread::WorkerThread(MigrationReportPacker& parent):
m_parent(parent) {
}

void MigrationReportPacker::WorkerThread::run(){
  while(1) {
    std::auto_ptr<MigrationReportPacker::Report> rep (m_parent.m_fifo.pop());
    rep->execute(m_parent);
  }
}

}}}}
