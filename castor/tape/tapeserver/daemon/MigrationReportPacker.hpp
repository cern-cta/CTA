/******************************************************************************
 *                      MigrationReportPacker.hpp
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

#pragma once

#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/MigrationJob.hpp"
#include "castor/tape/tapeserver/daemon/ClientInterface.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include <list>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
 
class MigrationReportPacker {
public:
  MigrationReportPacker(ClientInterface & tg);
  
  /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   */
  void reportCompletedJob(const tapegateway::FileToMigrateStruct& migratedFile);
  
  /**
   * Create into the MigrationReportPacker a report for the failled migration
   * of migratedFile
   * @param migratedFile the file which failled 
   */
  void reportFailedJob(const tapegateway::FileToMigrateStruct& migratedFile,const std::string& msg,int error_code);
     
   /**
   * Create into the MigrationReportPacker a report for the signaling a flusing on tape
   */
  void reportFlush();
  
  /**
   * Create into the MigrationReportPacker a report for the nominal end of session
   */
  void reportEndOfSession();
  
  /**
   * Create into the MigrationReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   */
  void reportEndOfSessionWithErrors(const std::string msg,int error_code);
  
  void startThreads() { m_workerThread.start(); }
  void waitThread() { m_workerThread.wait(); }
  
private:
  
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(MigrationReportPacker& packer)=0;
  };
  
  class ReportSuccessful :  public Report {
    const tapegateway::FileToMigrateStruct& m_migratedFile;
  public:
    ReportSuccessful(const tapegateway::FileToMigrateStruct& file): 
    m_migratedFile(file){}
    virtual void execute(MigrationReportPacker& _this);
  };
  
  class ReportFlush : public Report {
    public:
      void execute(MigrationReportPacker& _this);
  };
  
  class ReportError :  public Report {
    const tapegateway::FileToMigrateStruct& m_migratedFile;
    const std::string m_error_msg;
    const int m_error_code;
  public:
    ReportError(const tapegateway::FileToMigrateStruct& file,const std::string& msg,int error_code):
    m_migratedFile(file),m_error_msg(msg),m_error_code(error_code){}
    
    virtual void execute(MigrationReportPacker& _this);
  };
  
  class ReportEndofSession :  public Report {
  public:
    virtual void execute(MigrationReportPacker& _this);
  };
  
  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;
    int m_error_code;
  public:
    ReportEndofSessionWithErrors(const std::string msg,int error_code):
    m_message(msg),m_error_code(error_code){}

    virtual void execute(MigrationReportPacker& _this);
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
    MigrationReportPacker & m_parent;
  public:
    WorkerThread(MigrationReportPacker& parent);
    virtual void run();
  } m_workerThread;

  ClientInterface & m_client;
  
  castor::tape::threading::BlockingQueue<Report*> m_fifo;
  tapegateway::FileMigrationReportList m_listReports;
  castor::tape::threading::Mutex m_producterProtection;
};

}}}}
