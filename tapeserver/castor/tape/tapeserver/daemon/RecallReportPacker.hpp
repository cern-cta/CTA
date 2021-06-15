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

#include "tapeserver/castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"

#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {
  
class RecallReportPacker : public ReportPackerInterface<detail::Recall> {
public:
  /**
   * Constructor
   * @param tg the client to whom we report the success/failures
   * @param lc log context, copied du to threads
   */
  RecallReportPacker(cta::RetrieveMount *retrieveMount, cta::log::LogContext lc);
  
  virtual ~RecallReportPacker();
  
 /**
   * Create into the MigrationReportPacker a report for the successful migration
   * of migratedFile
   * @param migratedFile the file successfully migrated
   * @param checksum the checksum the DWT has computed for the file 
   */
  virtual void reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob);
  
  /**
   * Create into the MigrationReportPacker a report for the failed migration
   * of migratedFile
   * @param migratedFile the file which failed 
   * @param ex the reason for the failure
   */
  virtual void reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob, const cta::exception::Exception & ex);
       
  /**
   * Create into the MigrationReportPacker a report for the nominal end of session
   */
  virtual void reportEndOfSession();
  
  /**
   * Function for testing purposes. It is used to tell the report packer that this is the last report
   */
  virtual void reportTestGoingToEnd();
  
  /**
   * Create into the MigrationReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   */
  virtual void reportEndOfSessionWithErrors(const std::string msg,int error_code); 
  
  /**
   * Report the drive state and set it in the central drive register. This
   * function is to be used by the tape thread when running.
   * @param state the new drive state.
   */
  virtual void reportDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason = cta::nullopt);
  
  /**
   * Flag disk thread as done.
   */
  virtual void setDiskDone();
  
  /**
   * Flag tape thread as done. Set the drive status to draining if needed.
   */
  virtual void setTapeDone();
  
  void setTapeComplete();
  
  void setDiskComplete();
  
  bool isDiskDone();
  
  /**
   * Query the status of disk and tape threads (are they both done?).
   * @return true if both tape and disk threads are done.
   */
  virtual bool allThreadsDone();

  /**
   * Start the inner thread
   */
  void startThreads() { m_workerThread.start(); }
  
  /**
   * Stop the inner thread
   */
  void waitThread() { m_workerThread.wait(); }
  
  /**
   * Was there an error?
   */
  bool errorHappened();
  
private:
  //inner classes use to store content while receiving a report 
  class Report {
  public:
    virtual ~Report(){}
    virtual void execute(RecallReportPacker& packer)=0;
    virtual bool goingToEnd() {return false;}
  };
  class ReportTestGoingToEnd :  public Report {
  public:
    ReportTestGoingToEnd() {}
    void execute(RecallReportPacker& reportPacker) override {
    reportPacker.m_retrieveMount->diskComplete();
    reportPacker.m_retrieveMount->tapeComplete();}
    bool goingToEnd() override {return true;}
  };
  class ReportSuccessful :  public Report {
    /**
     * The successful retrieve job to be reported immediately
     */
    std::unique_ptr<cta::RetrieveJob> m_successfulRetrieveJob;
  public:
    ReportSuccessful(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob): 
    m_successfulRetrieveJob(std::move(successfulRetrieveJob)){}
    void execute(RecallReportPacker& reportPacker) override;
  };
  class ReportError : public Report {
    const std::string m_failureLog;
    /**
     * The failed retrieve job to be reported immediately
     */
    std::unique_ptr<cta::RetrieveJob> m_failedRetrieveJob;
  public:
    ReportError(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob, const std::string &failureLog):
    m_failureLog(failureLog), m_failedRetrieveJob(std::move(failedRetrieveJob)) {}

    void execute(RecallReportPacker& reportPacker) override;
  };
  
  class ReportDriveStatus : public Report {
    cta::common::dataStructures::DriveStatus m_status;
    cta::optional<std::string> m_reason;
    
  public:
    ReportDriveStatus(cta::common::dataStructures::DriveStatus status,const cta::optional<std::string> & reason): m_status(status), m_reason(reason) {}
    void execute(RecallReportPacker& reportPacker) override;
    bool goingToEnd() override;
  };
  
  class ReportEndofSession : public Report {
  public:
    ReportEndofSession(){}
    void execute(RecallReportPacker& reportPacker) override;
    bool goingToEnd() override;

  };
  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;
    int m_error_code;
  public:
    ReportEndofSessionWithErrors(std::string msg,int error_code):
    m_message(msg),m_error_code(error_code){}
  
    void execute(RecallReportPacker& reportPacker) override;
    bool goingToEnd() override;
  };
  
  class WorkerThread: public cta::threading::Thread {
    RecallReportPacker & m_parent;
  public:
    WorkerThread(RecallReportPacker& parent);
    void run() override;
  } m_workerThread;
  
  cta::threading::Mutex m_producterProtection;
  
  /** 
   * m_fifo is holding all the report waiting to be processed
   */
  cta::threading::BlockingQueue<Report*> m_fifo;
  
  /**
   * Is set as true as soon as we process a reportFailedJob
   * That we can do a sanity check to make sure we always call 
   * the right end of the session  
   */
  bool m_errorHappened;
  
  /**
   * The mount object used to send reports
   */
  cta::RetrieveMount * m_retrieveMount;
  
  /**
   * The successful reports that were pre-reported asynchronously.
   * They are collected and completed regularly.
   */
  std::queue<std::unique_ptr<cta::RetrieveJob> > m_successfulRetrieveJobs;
  
  /**
   * Tracking of the tape thread end
   */
  bool m_tapeThreadComplete;
  
  /**
   * Tracking of the disk thread end
   */
  bool m_diskThreadComplete;  
  
  cta::threading::Mutex m_mutex;

  /*
   * Proceed finish procedure for async execute for all reports.
   *  
   * @param reportedSuccessfuly The successful reports to check
   * @return The number of reports proceeded
   */
  void fullCheckAndFinishAsyncExecute();
  
  /*
   * The limit for successful reports to trigger flush.
   */
  const unsigned int RECALL_REPORT_PACKER_FLUSH_SIZE = 2000;
  
  /*
   * The time limit for successful reports to trigger flush.
   */
  const double RECALL_REPORT_PACKER_FLUSH_TIME = 180;
};

}}}}


