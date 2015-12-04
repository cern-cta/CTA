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

#include <memory>

namespace cta {
  class NameServer;
  
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
    ArchiveMount(NameServer & ns);

    /**
     * Constructor.
     *
     * @param dbMount The database representation of this mount.
     */
    ArchiveMount(NameServer & ns, std::unique_ptr<cta::SchedulerDatabase::ArchiveMount> dbMount);

  public:

    CTA_GENERATE_EXCEPTION_CLASS(WrongMountType);
    CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);

    /**
     * Returns The type of this tape mount.
     *
     * @return The type of this tape mount.
     */
    virtual MountType::Enum getMountType() const;

    /**
     * Returns the volume identifier of the tape to be mounted.
     *
     * @return The volume identifier of the tape to be mounted.
     */
    virtual std::string getVid() const;

    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    virtual std::string getMountTransactionId() const;

    /**
     * Indicates that the mount was completed.
     */
    virtual void complete();
    
    /**
     * Indicates that the mount was cancelled.
     */
    virtual void abort();
    
    /**
     * Report a drive status change
     */
    virtual void setDriveStatus(cta::DriveStatus status);

    CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);
    /**
     * Job factory
     *
     * @return A unique_ptr to the next archive job or NULL if there are no more
     * archive jobs left for this tape mount.
     */
    virtual std::unique_ptr<ArchiveJob> getNextJob();
    
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
    virtual uint32_t getNbFiles() const;
    
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
     * A reference to the name server.
     */
    NameServer & m_ns;

    /**
     * Internal tracking of the session completion
     */
    bool m_sessionRunning;
    
  }; // class ArchiveMount

} // namespace cta
