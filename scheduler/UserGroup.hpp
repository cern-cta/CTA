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

#include "scheduler/ConfigurationItem.hpp"
#include "scheduler/DriveQuota.hpp"
#include "scheduler/MountCriteria.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing a user group.
 */
class UserGroup: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  UserGroup();

  /**
   * Destructor.
   */
  ~UserGroup() throw();

  /**
   * Constructor.
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
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the user group.
   *
   * @return The name of the user group.
   */
  const std::string &getName() const throw();

  /**
   * Returns the tape drive quota for the user group when mounting tapes for
   * file archive.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file archive.
   */
  const DriveQuota &getArchiveDriveQuota() const throw();

  /**
   * Returns the tape drive quota for the user group when mounting tapes for
   * file retrieve.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file retrieve.
   */
  const DriveQuota &getRetrieveDriveQuota() const throw();

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file archive.
   */
  const MountCriteria &getArchiveMountCriteria() const throw();

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file retrieve.
   */
  const MountCriteria &getRetrieveMountCriteria() const throw();

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

}; // class UserGroup

} // namespace cta
