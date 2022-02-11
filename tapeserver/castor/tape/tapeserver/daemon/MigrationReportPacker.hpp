/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/threading/BlockingQueue.hpp"
/*#include "castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"*/
#include "tapeserver/castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
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
  MigrationReportPacker(cta::ArchiveMount *archiveMount, cta::log::LogContext&  lc);
  
  ~MigrationReportPacker();
    
  /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   * @param checksum the checksum we computed of the file we have just migrated
   * @param blockId The tape logical object ID of the first block of the header
   * of the file. This is 0 (instead of 1) for the first file on the tape (aka
   * fseq = 1).
   * @param lc log context provided by the calling thread.
   */
  virtual void reportCompletedJob(std::unique_ptr<cta::ArchiveJob> successfulArchiveJob, cta::log::LogContext & lc);
  
  /**
   * Create into the MigrationReportPacker a report for a skipped file. We left a placeholder on tape, so
   * writing can carry on, but this fSeq holds no data. In the mean time, the job has to count a failure.
   * @param skippedArchiveJob the failed file
   * @param ex the reason for the failure
   * @param lc log context provided by the calling thread.
   */
  virtual void reportSkippedJob(std::unique_ptr<cta::ArchiveJob> skippedArchiveJob, const std::string& failure, cta::log::LogContext & lc);
  /**
   * Create into the MigrationReportPacker a report for the failled migration
   * of migratedFile
   * @param migratedFile the file which failled 
   * @param ex the reason for the failure
   * @param lc log context provided by the calling thread.
   */
  virtual void reportFailedJob(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, const cta::exception::Exception& ex, cta::log::LogContext & lc);
     
   /**
    * Create into the MigrationReportPacker a report for the signaling a flusing on tape
    * @param compressStats 
    * @param lc log context provided by the calling thread.
    *
    */
  virtual void reportFlush(drive::compressionStats compressStats, cta::log::LogContext & lc);
  
  /**
   * Create into the MigrationReportPacker a report of reaching the end of the tape.
   * @param lc log context provided by the calling thread.
   */
  virtual void reportTapeFull(cta::log::LogContext & lc);
  
  /**
   * Report the drive state and set it in the central drive register. This
   * function is to be used by the tape thread when running.
   * @param state the new drive state.
   * @param lc log context provided by the calling thread.
   */
  virtual void reportDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason, cta::log::LogContext & lc);
  
  /**
   * Create into the MigrationReportPacker a report for the nominal end of session
   * @param lc log context provided by the calling thread.
   */
  virtual void reportEndOfSession(cta::log::LogContext & lc);
  
  /**
   * Function for testing purposes. It is used to tell the report packer that this is the last report
   * @param lc log context provided by the calling thread.
   */
  virtual void reportTestGoingToEnd(cta::log::LogContext & lc);
  
  /**
   * Create into the MigrationReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   * @param lc log context provided by the calling thread.
   */
  virtual void reportEndOfSessionWithErrors(const std::string msg,int error_code, cta::log::LogContext & lc);

  /**
   * Immediately report the end of session to the client.
   * @param msg The error message 
   * @param error_code The error code given by the drive
   * @param lc log context provided by the calling thread.
   */
  virtual void synchronousReportEndWithErrors(const std::string msg,int error_code, cta::log::LogContext & lc);
  
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
    void execute(MigrationReportPacker& reportPacker) override;
  };
  
  class ReportSkipped : public Report{
    const std::string m_failureLog;
    /**
     * The failed archive job we skipped
     */
    std::unique_ptr<cta::ArchiveJob> m_skippedArchiveJob;
  public:
    ReportSkipped(std::unique_ptr<cta::ArchiveJob> skippedArchiveJob, std::string &failureLog):
    m_failureLog(failureLog), m_skippedArchiveJob(std::move(skippedArchiveJob)) {}
    void execute(MigrationReportPacker& reportPacker) override;
  };
  
  class ReportTestGoingToEnd :  public Report {
  public:
    ReportTestGoingToEnd() {}
    virtual void execute(MigrationReportPacker& reportPacker) override {
      reportPacker.m_continue=false;
      reportPacker.m_lc.log(cta::log::DEBUG, "In MigrationReportPacker::ReportTestGoingToEnd::execute(): Reporting session complete.");
      reportPacker.m_archiveMount->complete();
    }
  };
  
  class ReportDriveStatus : public Report {
    cta::common::dataStructures::DriveStatus m_status;
    cta::optional<std::string> m_reason;
  public:
    ReportDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason): m_status(status),m_reason(reason) {}
    void execute(MigrationReportPacker& reportPacker) override;
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
      
      void execute(MigrationReportPacker& reportPacker) override;
  };
  class ReportTapeFull: public Report {
    public:
      ReportTapeFull() {}
      void execute(MigrationReportPacker& reportPacker) override;
  };
  class ReportError : public Report {
    const std::string m_failureLog;
    
    /**
     * The failed archive job to be reported immediately
     */
    std::unique_ptr<cta::ArchiveJob> m_failedArchiveJob;
  public:
    ReportError(std::unique_ptr<cta::ArchiveJob> failedArchiveJob, std::string &failureLog):
    m_failureLog(failureLog), m_failedArchiveJob(std::move(failedArchiveJob)){}
    
    void execute(MigrationReportPacker& reportPacker) override;
  };
  class ReportEndofSession : public Report {
  public:
    virtual void execute(MigrationReportPacker& reportPacker) override;
  };
  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;
    int m_errorCode;
  public:
    ReportEndofSessionWithErrors(std::string msg,int errorCode):
    m_message(msg),m_errorCode(errorCode){}
    
    void execute(MigrationReportPacker& reportPacker) override;
  };
  
  class WorkerThread: public cta::threading::Thread {
    MigrationReportPacker & m_parent;
  public:
    WorkerThread(MigrationReportPacker& parent);
    virtual void run();
  } m_workerThread;
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  cta::threading::BlockingQueue<std::unique_ptr<Report>> m_fifo;

  
  cta::threading::Mutex m_producterProtection;
  
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
  
  /**
   * The skipped files (or placeholders list)
   */
  std::queue<cta::catalogue::TapeItemWritten> m_skippedFiles;
};

}}}}
