/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "tapeserver/castor/tape/tapeserver/daemon/ReportPackerInterface.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Thread.hpp"
#include "common/threading/BlockingQueue.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"

#include <memory>
#include <utility>

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
  RecallReportPacker(cta::RetrieveMount* retrieveMount, cta::log::LogContext& lc);

  ~RecallReportPacker() override;

  /**
    * Create into the RecallReportPacker a report for the successful migration
    * of migratedFile
    * @param successfulRetrieveJob the file successfully retrieved
    * @param lc log context provided by the calling thread.
    */
  virtual void reportCompletedJob(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob, cta::log::LogContext& lc);

  /**
   * Create into the RecallReportPacker a report for the failed migration
   * of migratedFile
   * @param migratedFile the file which failed 
   * @param ex the reason for the failure
   * @param lc log context provided by the calling thread.
   */
  virtual void reportFailedJob(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob,
                               const cta::exception::Exception& ex,
                               cta::log::LogContext& lc);

  /**
   * Create into the RecallReportPacker a report for the nominal end of session
   * @param lc log context provided by the calling thread.
   */
  virtual void reportEndOfSession(cta::log::LogContext& lc);

  /**
   * Create into the RecallReportPacker a report for an erroneous end of session
   * @param msg The error message 
   * @param error_code The error code given by the drive
   * @param lc log context provided by the calling thread.
   */
  virtual void reportEndOfSessionWithErrors(const std::string& msg, cta::log::LogContext& lc);

  /**
   * Report the drive state and set it in the central drive register. This
   * function is to be used by the tape thread when running.
   * @param state the new drive state.
   * @param reason the comment to a change.
   * @param lc log context provided by the calling thread.
   */
  virtual void reportDriveStatus(cta::common::dataStructures::DriveStatus status,
                                 const std::optional<std::string>& reason,
                                 cta::log::LogContext& lc);

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
    virtual ~Report() = default;

    virtual void execute(RecallReportPacker& packer) = 0;

    virtual bool goingToEnd() { return false; }
  };

  class ReportSuccessful : public Report {
    /**
     * The successful retrieve job to be reported immediately
     */
    std::unique_ptr<cta::RetrieveJob> m_successfulRetrieveJob;

  public:
    explicit ReportSuccessful(std::unique_ptr<cta::RetrieveJob> successfulRetrieveJob) :
    m_successfulRetrieveJob(std::move(successfulRetrieveJob)) {}

    void execute(RecallReportPacker& reportPacker) override;
  };

  class ReportError : public Report {
    const std::string m_failureLog;
    /**
     * The failed retrieve job to be reported immediately
     */
    std::unique_ptr<cta::RetrieveJob> m_failedRetrieveJob;

  public:
    ReportError(std::unique_ptr<cta::RetrieveJob> failedRetrieveJob, std::string failureLog) :
    m_failureLog(std::move(failureLog)),
    m_failedRetrieveJob(std::move(failedRetrieveJob)) {}

    void execute(RecallReportPacker& reportPacker) override;
  };

  class ReportDriveStatus : public Report {
    cta::common::dataStructures::DriveStatus m_status;
    std::optional<std::string> m_reason;

  public:
    ReportDriveStatus(cta::common::dataStructures::DriveStatus status, std::optional<std::string> reason) :
    m_status(status),
    m_reason(std::move(reason)) {}

    void execute(RecallReportPacker& reportPacker) override;

    bool goingToEnd() override;
  };

  class ReportEndofSession : public Report {
  public:
    ReportEndofSession() = default;

    void execute(RecallReportPacker& reportPacker) override;

    bool goingToEnd() override;
  };

  class ReportEndofSessionWithErrors : public Report {
    std::string m_message;

  public:
    ReportEndofSessionWithErrors(std::string msg) : m_message(std::move(msg)) {}

    void execute(RecallReportPacker& reportPacker) override;

    bool goingToEnd() override;
  };

  class WorkerThread : public cta::threading::Thread {
    RecallReportPacker& m_parent;

  public:
    explicit WorkerThread(RecallReportPacker& parent);

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
  cta::RetrieveMount* m_retrieveMount;

  /**
   * The successful reports that were pre-reported asynchronously.
   * They are collected and completed regularly.
   */
  std::queue<std::unique_ptr<cta::RetrieveJob>> m_successfulRetrieveJobs;

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

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
