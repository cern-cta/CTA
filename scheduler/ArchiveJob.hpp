/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "common/Timer.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "catalogue/Catalogue.hpp"
#include "disk/DiskReporter.hpp"

#include <stdint.h>
#include <string>
#include <future>

namespace cta {

// Forward declaration
class ArchiveMount;
namespace eos {
class DiskReporterFactory;
}

/**
 * Class representing the transfer of a single copy of a remote file to tape.
 */
class ArchiveJob {

  /**
   * The ArchiveMount class is a friend so that it can call the private
   * constructor of ArchiveJob.
   */
  friend class ArchiveMount;
  friend class Scheduler;
  
protected:
  /**
   * Constructor.
   * 
   * @param mount the mount that generated this job
   * @param archiveFile informations about the file that we are storing
   * @param remotePathAndStatus location and properties of the remote file
   * @param tapeFileLocation the location within the tape
   */
  ArchiveJob(
  ArchiveMount *mount,
  catalogue::Catalogue & catalogue,
  const common::dataStructures::ArchiveFile &archiveFile,
  const std::string &srcURL,
  const common::dataStructures::TapeFile &tapeFile);

public:

  /**
   * Destructor.
   */
  virtual ~ArchiveJob() throw();
  
  CTA_GENERATE_EXCEPTION_CLASS(BlockIdNotSet);
  CTA_GENERATE_EXCEPTION_CLASS(ChecksumNotSet);
  
  /**
   * Start an asynchronous update for a batch of jobs and then make sure they complete.
   */
  static void asyncSetJobsBatchSucceed(std::list<std::unique_ptr<cta::ArchiveJob>> & jobs);
  
  /**
   * Get the report time (in seconds).
   */
  double reportTime();
  
  /**
   * Validate that archiveFile and tapeFile fields are set correctly for archive
   * request.
   * Throw appropriate exception if there is any problem.
   */
  virtual void validate();
  
  /**
   * Validate that archiveFile and tapeFile fields are set correctly for archive
   * request.
   * @return The tapeFileWritten event for the catalog update.
   */
  virtual cta::catalogue::TapeItemWrittenPointer validateAndGetTapeFileWritten();
  
  /**
   * Triggers a scheduler update following the failure of the job. Retry policy will
   * be applied by the scheduler.
   */
  virtual void transferFailed(const std::string &failureReason, log::LogContext & lc);
  
  /**
   * Get the URL used for reporting
   * @return The URL used to report to the disk system.
   */
  virtual std::string exceptionThrowingReportURL();

  /**
   * Same as exceptionThrowingReportURL() except it doesn't throw exceptions.
   * Errors are returned in the output string.
   * @return The URL used to report to the disk system.
   */
  virtual std::string reportURL() noexcept;

  /**
   * Get the report type.
   * @return the type of report (success or failure), as a string
   */
  virtual std::string reportType();
  
  /**
   * Triggers a scheduler update following the failure of the report. Retry policy will
   * be applied by the scheduler. Failure to report success will also be a failure reason.
   */
  virtual void reportFailed(const std::string &failureReason, log::LogContext & lc);
  

private:
  std::unique_ptr<cta::SchedulerDatabase::ArchiveJob> m_dbJob;
  
  /**
   * The reporter for the job. TODO: this should be generic and fed with a factory.
   */
  std::unique_ptr<cta::disk::DiskReporter> m_reporter;
  
  /**
   * Report time measurement.
   */
  utils::Timer m_reporterTimer;
  
  /**
   * The mount that generated this job
   */
  ArchiveMount *m_mount = nullptr;

  /**
   * Get access to the mount or throw exception if we do not have one
   */
  ArchiveMount &getMount();

  /**
   * Reference to the name server
   */
  catalogue::Catalogue &m_catalogue;
  
  /**
   * State for the asynchronous report to the client. 
   */
  std::promise<void> m_reporterState;
      
public:
    
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  
  /**
   * The NS archive file information
   */
  common::dataStructures::ArchiveFile archiveFile;
  
  /**
   * The remote file information
   */
  std::string srcURL;
  
  /**
   * The file archive result for the NS
   */
  common::dataStructures::TapeFile tapeFile;
  
  /**
   * Wait for the reporterState is set by the reporting thread.
   */
  virtual void waitForReporting();

}; // class ArchiveJob

} // namespace cta
