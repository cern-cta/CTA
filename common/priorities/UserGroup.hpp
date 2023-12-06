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

#include "common/CreationLog.hpp"
#include "common/priorities/DriveQuota.hpp"
#include "common/priorities/MountCriteria.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing a user group.
 */
class UserGroup {
public:
  /**
   * Constructor
   */
  UserGroup() = default;

  /**
   * Destructor
   */
  ~UserGroup() = default;

  /**
   * Constructor
   *
   * @param name The name of the user group.
   * @param archiveDriveQuota The tape drive quota for the user group when
   * mounting tapes for file archive.
   * @param retrieveDriveQuota The tape drive quota for the user group when
   * mounting tapes for file retrieve.
   * @param archiveMountCriteria The criteria of the user group to be met in
   * order to justify mounting a tape for file archive.
   * @param retrieveMountCriteria The criteria of the user group to be met in
   * order to justify mounting a tape for file retrieve.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  UserGroup(
    const std::string &name,
    const DriveQuota &archiveDriveQuota,
    const DriveQuota &retrieveDriveQuota,
    const MountCriteria &archiveMountCriteria,
    const MountCriteria &retrieveMountCriteria,
    const CreationLog & creationLog);

  /**
   * Returns the name of the user group.
   *
   * @return The name of the user group.
   */
  const std::string &getName() const noexcept;

  /**
   * Returns the tape drive quota for the user group when mounting tapes for
   * file archive.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file archive.
   */
  const DriveQuota &getArchiveDriveQuota() const noexcept;

  /**
   * Returns the tape drive quota for the user group when mounting tapes for
   * file retrieve.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file retrieve.
   */
  const DriveQuota &getRetrieveDriveQuota() const noexcept;

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file archive.
   */
  const MountCriteria &getArchiveMountCriteria() const noexcept;

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file retrieve.
   */
  const MountCriteria &getRetrieveMountCriteria() const noexcept;

private:
  /**
   * The name of the user group.
   */
  std::string m_name;

  /**
   * The tape drive quota for the user group when mounting tapes for file
   * archive.
   */
  DriveQuota m_archiveDriveQuota;

  /**
   * The tape drive quota for the user group when mounting tapes for file
   * retrieve.
   */
  DriveQuota m_retrieveDriveQuota;

  /**
   * The criteria of the user group to be met in order to justify mounting a
   * tape for file archive.
   */
  MountCriteria m_archiveMountCriteria;

  /**
   * The criteria of the user group to be met in order to justify mounting a
   * tape for file retrieve.
   */
  MountCriteria m_retrieveMountCriteria;
  
  /**
   * The when, who, why of the group's creation
   */
  CreationLog m_creationLog;
};

} // namespace cta
