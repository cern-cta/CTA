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

#include <list>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "common/exception/Exception.hpp"
#include "disk/DiskReporterFactory.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

/**
  * The class driving a retrieve mount.
  * The class only has private constructors as it is instanciated by
  * the Scheduler class.
  */
class RetrieveMount: public TapeMount {
  friend class Scheduler;
protected:
  /**
    * Constructor.
    * @param catalogue The file catalogue interface.
    */
  explicit RetrieveMount(cta::catalogue::Catalogue &catalogue);

  /**
    * Constructor.
    * @param catalogue The file catalogue interface.
    * @param dbMount The database representation of this mount.
    */
  RetrieveMount(cta::catalogue::Catalogue &catalogue, std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> dbMount);

public:
  CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

  /**
    * Destructor
    */
  virtual ~RetrieveMount()  = default;

  /**
    * Returns The type of this tape mount.
    *
    * @return The type of this tape mount.
    */
  cta::common::dataStructures::MountType getMountType() const override;

  /**
    * Returns the volume identifier of the tape to be mounted.
    *
    * @return The volume identifier of the tape to be mounted.
    */
  std::string getVid() const override;

  /**
    * Returns the (optional) activity for this mount.
    *
    * @return
    */
  std::optional<std::string> getActivity() const override;


  /**
    * Returns the mount transaction id.
    *
    * @return The mount transaction id.
    */
  std::string getMountTransactionId() const override;

  /**
    * Returns the mount transaction id.
    *
    * @return The mount transaction id.
    */
  uint32_t getNbFiles() const override;

    /**
    * Returns the tape pool of the tape to be mounted
    * @return The tape pool of the tape to be mounted
    */
  std::string getPoolName() const override;

  /**
    * Returns the virtual organization in which the tape
    * belongs
    * @return the vo in which the tape belongs
    */
  std::string getVo() const override;

  /**
    * Returns the media type of the tape
    * @return de media type of the tape
    */
  std::string getMediaType() const override;

  /**
    * Returns the vendor of the tape
    * @return the vendor of the tape
    */
  std::string getVendor() const override;

  /**
    * Returns the tape drive
    * @return the tape drive
    */
  virtual std::string getDrive() const;

  /**
  * Returns the capacity in bytes of the tape
  * @return the capacity in bytes of the tape
  */
  uint64_t getCapacityInBytes() const override;

  /**
    * Returns the media format of the tape
    * @return the media format of the tape
    */
  cta::common::dataStructures::Label::Format getLabelFormat() const override;

  /**
    * Returns the encryption key ID of the tape
    * @return the encryption key ID of the tape
    */
  std::optional<std::string> getEncryptionKeyName() const override;

  /**
    * Report a drive status change
    */
  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      const std::optional<std::string> &reason = std::nullopt) override;

  /**
    * Report a tape session statistics
    */
  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override;

  /**
    * Report a tape mounted event
    * @param logContext
    */
  void setTapeMounted(log::LogContext &logContext) const override;

  /**
    * Indicates that the disk thread of the mount was completed. This
    * will implicitly trigger the transition from DrainingToDisk to Up if necessary.
    */
  virtual void diskComplete();

  /**
    * Indicates that the tape thread of the mount was completed. This
    * will implicitly trigger the transition from Unmounting to either Up or
    * DrainingToDisk, depending on the the disk thread's status.
    */
  virtual void tapeComplete();

  /**
    * Indicates that the we should cancel the mount (equivalent to diskComplete
    * + tapeComplete).
    */
  void complete() override;

  /**
    * Tests whether all threads are complete
    * @return true if both tape and disk are complete.
    */
  virtual bool bothSidesComplete();

  CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);

  /**
    * Batch job factory
    *
    * @param filesRequested the number of files requested
    * @param bytesRequested the number of bytes requested
    * @param logContext
    * @return a list of unique_ptr to the next retrieve jobs. The list is empty
    * when no more jobs can be found. Will return jobs (if available) until one
    * of the 2 criteria is fulfilled.
    */
  virtual std::list<std::unique_ptr<RetrieveJob>> getNextJobBatch(uint64_t filesRequested,
    uint64_t bytesRequested, log::LogContext &logContext);

  /**
    * Requeues a batch of jobs in their respective queues
    * @param jobs The job batch
    * @param logContext
    */
  virtual void requeueJobBatch(std::vector<std::unique_ptr<cta::RetrieveJob>> &jobs, log::LogContext &logContext);

  /**
    * Puts the queue associated to this retrieve mount to sleep
    */
  virtual void putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime, log::LogContext &logContext);


  /**
    * Reserves space in the disk space buffer to the drive of this mount
    * @param request the disk space reservation request
    * @param logContext
    */
  virtual bool reserveDiskSpace(const cta::DiskSpaceReservationRequest &request, log::LogContext& logContext);

  /**
    * Performs the disk space reservation logic for the request and mount, but does not reserve space in the catalogue
    * @param request the disk space reservation request
    * @param logContext
    */
  virtual bool testReserveDiskSpace(const cta::DiskSpaceReservationRequest &request, log::LogContext& logContext);


  /**
    * Wait and complete reporting of a batch of jobs successes. The per jobs handling has
    * already been launched in the background using cta::RetrieveJob::asyncComplete().
    * This function will check completion of those async completes and then proceed
    * with any necessary common handling.
    *
    * @param successfulRetrieveJobs the jobs to report
    * @param logContext
    */
  virtual void flushAsyncSuccessReports(std::queue<std::unique_ptr<cta::RetrieveJob> > & successfulRetrieveJobs, cta::log::LogContext &logContext);


  /**
    * Creates a disk reporter for the retrieve job (this is a wrapper).
    * @param URL: report address
    * @return pointer to the reporter created.
    */
  disk::DiskReporter * createDiskReporter(std::string & URL);

  void setExternalFreeDiskSpaceScript(const std::string& name);

  /**
    * Adds a disk system to the list of disk systems to skip for this mount
    * @param diskSystem The disk system
    */
  virtual void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip &diskSystem);

private:
  /**
    * The database representation of this mount.
    */
  std::unique_ptr<cta::SchedulerDatabase::RetrieveMount> m_dbMount;

  /**
    * Internal tracking of the session completion
    */
  bool m_sessionRunning;

  /**
    * Internal tracking of the tape thread
    */
  bool m_tapeRunning;

  /**
    * Internal tracking of the disk thread
    */
  bool m_diskRunning;

  /** An initialized-once factory for archive reports (indirectly used by ArchiveJobs) */
  disk::DiskReporterFactory m_reporterFactory;

  /**
    * Internal tracking of the full disk systems. It is one strike out (for the mount duration).
    */
  std::set<std::string> m_fullDiskSystems;

  /**
    * A pointer to the file catalogue.
    */
  cta::catalogue::Catalogue &m_catalogue;

  /**
    * The name of the script that will be executed
    * to get the free disk space in the Retrieve space
    */
  std::string m_externalFreeDiskSpaceScript;
};  // class RetrieveMount

} // namespace cta
