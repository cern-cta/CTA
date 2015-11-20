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

#pragma once

#include "castor/server/BlockingQueue.hpp"
#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/ArchiveJob.hpp"
#include <list>
#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
 
class MigrationReportPacker : public ReportPackerInterface<detail::Migration> {
public:
  /**
   * @param tg The client who is asking for a migration of his files 
   * and to whom we have to report to the status of the operations.
   */
  MigrationReportPacker(cta::ArchiveMount *archiveMount, log::LogContext& lc);
  
  ~MigrationReportPacker();
    
  /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   * @param checksum the checksum we computed of the file we have just migrated
   * @param blockId The tape logical object ID of the first block of the header
   * of the file. This is 0 (instead of 1) for the first file on the tape (aka
   * fseq = 1).
   */
  virtual void reportCompletedJob(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob);
  
  /**
   * Create into the MigrationReportPacker a report for the failled migration
   * of migratedFile
   * @param migratedFile the file which failled 
   * @param ex the reason for the failure 
   */
  virtual void reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, const castor::exception::Exception& ex);
     
   /**
    * Create into the MigrationReportPacker a report for the signaling a flusing on tape
    * @param compressStats 
    * 
    */
  virtual void reportFlush(drive::compressionStats compressStats);
  
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

  /**
   * Immediately report the end of session to the client.
   * @param msg The error message 
   * @param error_code The error code given by the drive
   */
  virtual void synchronousReportEndWithErrors(const std::string msg,int error_code);
  
  void startThreads() { m_workerThread.start(); }
  void waitThread() { m_workerThread.wait(); }
  
private:
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(MigrationReportPacker& packer)=0;
  };
  class ReportSuccessful :  public Report {
    /**
     * The successful archive job to be pushed in the report packer queue and reported later
     */
    std::unique_ptr<cta::ArchiveJob> m_successfulArchiveJob;
  public:
    ReportSuccessful(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob): 
    m_successfulArchiveJob(std::move(successfulArchiveJob)) {}
    virtual void execute(MigrationReportPacker& reportPacker);
  };
  class ReportFlush : public Report {
    drive::compressionStats m_compressStats;
    
    public:
    /* We only can compute the compressed size once we have flushed on the drive
     * We can get from the drive the number of byte it really wrote to tape
     * @param nbByte the number of byte it really wrote to tape between 
     * this flush and the previous one
     *  */
      ReportFlush(drive::compressionStats compressStats):m_compressStats(compressStats){}
      
      void execute(MigrationReportPacker& reportPacker);
  };
  class ReportError : public Report {
    const castor::exception::Exception m_ex;
    
    /**
     * The failed archive job to be reported immediately
     */
    std::unique_ptr<cta::ArchiveJob> m_failedArchiveJob;
  public:
    ReportError(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, const castor::exception::Exception &ex):
    m_ex(ex), m_failedArchiveJob(std::move(failedArchiveJob)){}
    
    virtual void execute(MigrationReportPacker& reportPacker);
  };
  class ReportEndofSession : public Report {
  public:
    virtual void execute(MigrationReportPacker& reportPacker);
  };
  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;
    int m_errorCode;
  public:
    ReportEndofSessionWithErrors(std::string msg,int errorCode):
    m_message(msg),m_errorCode(errorCode){}
    
    virtual void execute(MigrationReportPacker& reportPacker);
  };
  
  class WorkerThread: public castor::server::Thread {
    MigrationReportPacker & m_parent;
  public:
    WorkerThread(MigrationReportPacker& parent);
    virtual void run();
  } m_workerThread;
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  castor::server::BlockingQueue<Report*> m_fifo;


  castor::server::Mutex m_producterProtection;
  
  /** 
   * Sanity check variable to register if an error has happened 
   * Is set at true as soon as a ReportError has been processed.
   */
  bool m_errorHappened;
  
  /* bool to keep the inner thread running. Is set at false 
   * when a end of session (with error) is called
   */
  bool m_continue;
  
  /**
   * The mount object used to send reports
   */
  cta::ArchiveMount * m_archiveMount;
  
  /**
   * The successful archive jobs to be reported when flushing
   */
  std::queue<std::unique_ptr<cta::ArchiveJob> > m_successfulArchiveJobs;
};

}}}}
