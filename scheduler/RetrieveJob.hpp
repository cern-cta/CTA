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

#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "scheduler/PositioningMethod.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <string>
#include <limits>
#include <memory>

namespace castor::tape::tapeserver::daemon {
  class TapeReadTask;
}

namespace cta {

class RetrieveMount;

/**
 * The transfer of a single copy of a tape file to a remote file.
 */
class RetrieveJob {
  /**
   * The RetrieveMount class is a friend so that it can call the private
   * constructor of RetrieveJob.
   */
  friend class RetrieveMount;
  friend class Scheduler;
  friend class castor::tape::tapeserver::daemon::TapeReadTask;

public:
  /**
   * Constructor
   *
   * @param mount the mount that generated this job
   * @param archiveFile informations about the file that we are storing
   * @param remotePathAndStatus location and properties of the remote file
   * @param tapeFileLocation the location of the tape file
   * @param positioningMethod the positioning method
   */
  RetrieveJob(RetrieveMount* mount,
    const common::dataStructures::RetrieveRequest& retrieveRequest,
    const common::dataStructures::ArchiveFile& archiveFile,
    const uint64_t selectedCopyNb,
    const PositioningMethod positioningMethod) :
      m_mount(mount),
      retrieveRequest(retrieveRequest),
      archiveFile(archiveFile),
      selectedCopyNb(selectedCopyNb),
      positioningMethod(positioningMethod),
      transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) { }

  RetrieveJob(RetrieveMount* mount,
              std::unique_ptr<SchedulerDatabase::RetrieveJob> dbJob)
      : m_dbJob(std::move(dbJob)),
        m_mount(mount),
        retrieveRequest(m_dbJob->retrieveRequest),
        archiveFile(m_dbJob->archiveFile),
        selectedCopyNb(m_dbJob->selectedCopyNb),
        positioningMethod(PositioningMethod::ByBlock),
        transferredSize(std::numeric_limits<decltype(transferredSize)>::max()) { }

  /**
   * Destructor
   */
  virtual ~RetrieveJob() = default;

private:
  std::unique_ptr<cta::SchedulerDatabase::RetrieveJob> m_dbJob;
  
  /**
   * The mount that generated this job
   */
  RetrieveMount *m_mount;

public:
  /**
   * Asynchronously indicates to the backend that the job was successful. 
   * The checksum and the size of the transfer should already stored in the 
   * object beforehand. Result setting and calling complete are done in 2 
   * different threads (disk write and reporter thread, respectively).
   * Completion will be checked implicitly in RetrieveMount::setJobBatchTransferred()
   */
  virtual void asyncSetSuccessful();
  /**
   * Returns the SchedulerDatabase::RetrieveJob jobID
   * Serves PGSCHED implementation
   * @return jobID as a string for SQL queries
   */
  virtual std::string getJobID();
  /**
   * Triggers a scheduler update following the failure of the job. Retry policy will
   * be applied by the scheduler.
   */
  virtual void transferFailed(const std::string &failureReason, log::LogContext &lc);

  /**
   * Get the URL used for reporting
   * @return The URL used to report to the disk system.
   */
  virtual std::string exceptionThrowingReportURL();

  /**
   * Get the report type.
   * @return the type of report (success or failure), as a string
   */
  virtual std::string reportType();
  
  /**
   * Triggers a scheduler update following the failure of the report. Retry policy will
   * be applied by the scheduler.
   */
  virtual void reportFailed(const std::string &failureReason, log::LogContext &lc);

  /**
   * Returning the name of the disk system associated with the scheduler database RetrieveJob
   */
  virtual std::optional<std::string> diskSystemName();

  /**
   * Helper function returning a reference to the currently selected tape file.
   */
  common::dataStructures::TapeFile & selectedTapeFile();

  /**
   * Helper function returning a reference to the currently selected tape file (const variant).
   */
  const common::dataStructures::TapeFile & selectedTapeFile() const;
  
  /**
   * The mount to which the job belongs.
   */
  //RetrieveMount &mount;  
  
  /**
   * The NS archive file information
   */
  common::dataStructures::RetrieveRequest retrieveRequest;
  
  /**
   * The full information about the file
   */
  common::dataStructures::ArchiveFile archiveFile;
                
  /**
   * CopyNb of the selected tape file
   */
  uint64_t selectedCopyNb;
  
  /**
   * The positioning method
   */
  PositioningMethod positioningMethod;
  
  /**
   * The checksum type of the transferred data. This should be set before calling 
   * complete()
   */
  std::string transferredChecksumType;
  
  /**
   * The checksum value of the transferred data. This should be set before calling 
   * complete()
   */
  std::string transferredChecksumValue;
  
  /**
   * The size of the transferred data. This should be set before calling 
   * complete().
   */
  uint64_t transferredSize;
};

} // namespace cta
