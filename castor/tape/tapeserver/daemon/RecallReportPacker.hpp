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

#pragma once

#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/log/LogContext.hpp"
#include "castor/tape/tapeserver/client/ClientInterface.hpp"
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/BlockingQueue.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class RecallReportPacker : protected ReportPackerInterface<detail::Recall> {
public:
  RecallReportPacker(client::ClientInterface & tg,unsigned int reportFilePeriod,
          log::LogContext lc);
  
  ~RecallReportPacker();
  
  /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   */
  virtual void reportCompletedJob(const FileStruct& recalledFile,
  unsigned long checksum);
  
  /**
   * Create into the MigrationReportPacker a report for the failed migration
   * of migratedFile
   * @param migratedFile the file which failed 
   */
  virtual void reportFailedJob(const FileStruct & recalledFile,const std::string& msg,int error_code);
       
  /**
   * Create into the MigrationReportPacker a report for the nominal end of session
   */
  virtual void reportEndOfSession();
  
  /**
   * Create into the MigrationReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   */
  virtual void reportEndOfSessionWithErrors(const std::string msg,int error_code);
  
  void startThreads() { m_workerThread.start(); }
  void waitThread() { m_workerThread.wait(); }
  
private:
  class Report {
    const bool m_endNear;
  public:
    Report(bool b):m_endNear(b){}
    virtual ~Report(){}
    virtual void execute(RecallReportPacker& packer)=0;
    bool goingToEnd() const {return m_endNear;};
  };
  class ReportSuccessful :  public Report {
    const FileStruct m_migratedFile;
    unsigned long m_checksum;
  public:
    ReportSuccessful(const FileStruct& file,unsigned long checksum): 
    Report(false),m_migratedFile(file),m_checksum(checksum){}
    virtual void execute(RecallReportPacker& _this);
  };
  class ReportError : public Report {
    const FileStruct m_migratedFile;
    const std::string m_error_msg;
    const int m_error_code;
  public:
    ReportError(const FileStruct& file,std::string msg,int error_code):
    Report(false),m_migratedFile(file),m_error_msg(msg),m_error_code(error_code){}

    virtual void execute(RecallReportPacker& _this);
  };
  class ReportEndofSession : public Report {
  public:
    ReportEndofSession():Report(true){}
    virtual void execute(RecallReportPacker& _this);
  };
  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;
    int m_error_code;
  public:
    ReportEndofSessionWithErrors(std::string msg,int error_code):
    Report(true),m_message(msg),m_error_code(error_code){}
  
    virtual void execute(RecallReportPacker& _this);
  };
  
  class WorkerThread: public castor::tape::threading::Thread {
    RecallReportPacker & m_parent;
  public:
    WorkerThread(RecallReportPacker& parent);
    virtual void run();
  } m_workerThread;
  
  void flush();
  
  castor::tape::threading::Mutex m_producterProtection;
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  castor::tape::threading::BlockingQueue<Report*> m_fifo;
  
  unsigned int m_reportFilePeriod;
  bool m_errorHappened;
};

}}}}


