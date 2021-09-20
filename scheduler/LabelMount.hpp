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
#include "scheduler/SchedulerDatabase.hpp"
#include "scheduler/TapeMount.hpp"
#include "catalogue/Catalogue.hpp"

#include <memory>
#include <atomic>

namespace cta {
  /**
   * The class driving a retrieve mount.
   * The class only has private constructors as it is instanciated by
   * the Scheduler class.
   */
  class LabelMount: public TapeMount {
    friend class Scheduler;
  protected:

    /**
     * Constructor.
     */
    LabelMount(catalogue::Catalogue & catalogue);

    /**
     * Constructor.
     *
     * @param dbMount The database representation of this mount.
     */
    LabelMount(catalogue::Catalogue & catalogue, std::unique_ptr<cta::SchedulerDatabase::LabelMount> dbMount);

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

    CTA_GENERATE_EXCEPTION_CLASS(SessionNotRunning);

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
     * Destructor.
     */
    virtual ~LabelMount() throw();

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

  }; // class ArchiveMount

} // namespace cta
