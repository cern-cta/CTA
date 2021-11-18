/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"
#include "catalogue/Catalogue.hpp"
#include "disk/DiskReporterFactory.hpp"

#include <memory>
#include <atomic>
#include <queue>

namespace cta {
  /**
   * The class driving a retrieve mount.
   * The class only has private constructors as it is instantiated by
   * the Scheduler class.
   */
  class ArchiveMount: public TapeMount {
    friend class Scheduler;
  protected:

    /**
     * Constructor.
     *
     * @param catalogue The file catalogue interface.
     */
    ArchiveMount(catalogue::Catalogue & catalogue);

    /**
     * Constructor.
     *
     * @param catalogue The file catalogue interface.
     * @param dbMount The database representation of this mount.
     */
    ArchiveMount(catalogue::Catalogue & catalogue, std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbMount);

  public:

    CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
    CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
    CTA_GENERATE_EXCEPTION_CLASS(FailedReportCatalogueUpdate);
    CTA_GENERATE_EXCEPTION_CLASS(FailedReportMoveToQueue);

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
     * Returns the drive name
     * @return The drive name
     */
    std::string getDrive() const;

    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    std::string getMountTransactionId() const override;

    /**
     * Return nullopt as activities are for retrieve mounts;
     *
     * @return nullopt.
     */
    optional<std::string> getActivity() const override { return nullopt; }


    /**
     * Indicates that the mount was completed.
     * This function is overridden in MockArchiveMount for unit tests.
     */
    virtual void complete();

    /**
     * Indicates that the mount was cancelled.
     */
    void abort(const std::string& reason) override;

    /**
     * Report a drive status change
     */
    void setDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason = cta::nullopt) override;

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
     * Report that the tape is full.
     */
    void setTapeFull();

    CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);
    /**
     * Batch job factory
     *
     * @param filesRequested the number of files requested
     * @param bytesRequested the number of bytes requested
     * @param logContext
     * @return a list of unique_ptr to the next archive jobs. The list is empty
     * when no more jobs can be found. Will return jobs (if available) until one
     * of the 2 criteria is fulfilled.
     */
    std::list<std::unique_ptr<ArchiveJob>> getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext &logContext);

    /**
     * Report a batch of jobs successes. The reporting will be asynchronous behind
     * the scenes.
     *
     * @param successfulArchiveJobs the jobs to report
     * @param logContext
     */
    virtual void reportJobsBatchTransferred (std::queue<std::unique_ptr<cta::ArchiveJob> > & successfulArchiveJobs,
        std::queue<cta::catalogue::TapeItemWritten> & skippedFiles, std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& failedToReportArchiveJobs, cta::log::LogContext &logContext);

    /**
     * Returns the tape pool of the tape to be mounted.
     *
     * @return The tape pool of the tape to be mounted.
     */
    std::string getPoolName() const;

    /**
     * Returns the virtual organization of the tape to be mounted
     * @return the vo of the tape to be mounted
     */
    std::string getVo() const;

    /**
     * Returns the media type of the tape
     * @return de media type of the tape
     */
    std::string getMediaType() const;

    /**
     * Returns the vendor of the tape
     * @return the vendor of the tape
     */
    std::string getVendor() const;

    /**
     * Returns the capacity in bytes of the tape
     * @return the capacity in bytes of the tape
     */
    uint64_t getCapacityInBytes() const;

    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    uint32_t getNbFiles() const override;

    /**
     * Creates a disk reporter for the ArchiveJob (this is a wrapper).
     * @param URL: report address
     * @param reporterState void promise to be set when the report is done asynchronously.
     * @return pointer to the reporter created.
     */
    disk::DiskReporter * createDiskReporter(std::string & URL);

    /**
     * Update the catalog with a set of TapeFileWritten events.
     *
     * @param tapeFilesWritten The set of report events for the catalog update.
     */
    void updateCatalogueWithTapeFilesWritten(const std::set<cta::catalogue::TapeItemWrittenPointer> &tapeFilesWritten);

    /**
     * Destructor.
     */
    virtual ~ArchiveMount() throw();

  protected:

    /**
     * The database representation of this mount.
     */
    std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> m_dbMount;

    /**
     * A reference to the file catalogue.
     */
    catalogue::Catalogue & m_catalogue;

    /**
     * Internal tracking of the session completion
     */
    std::atomic<bool> m_sessionRunning;

  private:
    /** An initialized-once factory for archive reports (indirectly used by ArchiveJobs) */
    disk::DiskReporterFactory m_reporterFactory;
  }; // class ArchiveMount

} // namespace cta
