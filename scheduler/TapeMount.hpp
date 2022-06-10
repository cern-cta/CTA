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

#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/DriveStatus.hpp"

#include "common/dataStructures/LabelFormat.hpp"
#include "common/log/LogContext.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/TapeSessionStats.hpp"

#include <optional>
#include <string>

namespace cta {
  /**
   * A placeholder class from which will derive ArchiveSession and RetrieveSession.
   * It's just here to allow RTTI.
   */
  class TapeMount {
  public:

    /**
     * Returns The type of this tape mount.
     *
     * @return The type of this tape mount.
     */
    virtual cta::common::dataStructures::MountType getMountType() const = 0;

    /**
     * Returns the volume identifier of the tape to be mounted.
     *
     * @return The volume identifier of the tape to be mounted.
     */
    virtual std::string getVid() const = 0;

    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    virtual std::string getMountTransactionId() const = 0;

    /**
     * Return the activity this mount is running for.
     *
     * @return optional, populated with the activity name if appropriate.
     */

    virtual std::optional<std::string> getActivity() const = 0;

    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    virtual uint32_t getNbFiles() const = 0;

    virtual std::string getVo() const = 0;

    virtual std::string getMediaType() const = 0;

    virtual std::string getVendor() const = 0;

    virtual cta::common::dataStructures::Label::Format getLabelFormat() const = 0;

    virtual std::string getPoolName() const = 0;

    /**
    * Returns the capacity in bytes of the tape
    * @return the capacity in bytes of the tape
    */
    virtual uint64_t getCapacityInBytes() const = 0;

    /**
     * Indicates that the mount was completed.
     */
    virtual void complete() = 0;

    /**
     * Report a drive status change
     */
    virtual void setDriveStatus(cta::common::dataStructures::DriveStatus status, const std::optional<std::string> & reason = std::nullopt) = 0;

    /**
     * Report a tape session statistics
     */
    virtual void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) = 0;

    /**
     * Report a tape mounted event
     * @param LogContext
     */
    virtual void setTapeMounted(cta::log::LogContext &logContext) const = 0;

    /**
     * Destructor.
     */
    virtual ~TapeMount() noexcept;

  }; // class TapeMount

}
