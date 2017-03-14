/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "scheduler/ArchiveJob.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"
#include "catalogue/Catalogue.hpp"
#include "eos/DiskReporterFactory.hpp"

#include <memory>
#include <atomic>

namespace cta {
  /**
   * The class driving a retrieve mount.
   * The class only has private constructors as it is instanciated by
   * the Scheduler class.
   */
  class ArchiveMount: public TapeMount {
    friend class Scheduler;
  protected:

    /**
     * Constructor.
     */
    ArchiveMount(catalogue::Catalogue & catalogue);

    /**
     * Constructor.
     *
     * @param dbMount The database representation of this mount.
     */
    ArchiveMount(catalogue::Catalogue & catalogue, std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbMount);

  public:

    CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
    CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

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
     * Indicates that the mount was completed.
     * This function is overridden in MockArchiveMount for unit tests.
     */
    virtual void complete();
    
    /**
     * Indicates that the mount was cancelled.
     */
    void abort() override;
    
    /**
     * Report a drive status change
     */
    void setDriveStatus(cta::common::dataStructures::DriveStatus status) override;
    
    /**
     * Report that the tape is full.
     */
    virtual void setTapeFull();

    CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);
    /**
     * Job factory
     *
     * @return A unique_ptr to the next archive job or NULL if there are no more
     * archive jobs left for this tape mount.
     */
    virtual std::unique_ptr<ArchiveJob> getNextJob(log::LogContext &logContext);
    
    /**
     * Returns the tape pool of the tape to be mounted.
     *
     * @return The tape pool of the tape to be mounted.
     */
    virtual std::string getPoolName() const;
    
    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    uint32_t getNbFiles() const override;
    
    /**
     * Creates a disk reporter for the ArchiveJob (this is a wrapper).
     * @param URL: report address
     * @return poitner to the reporter created.
     */
    eos::DiskReporter * createDiskReporter(std::string & URL);
    
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
    eos::DiskReporterFactory m_reporterFactory;
  }; // class ArchiveMount

} // namespace cta
