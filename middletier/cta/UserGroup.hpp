#pragma once

#include "cta/ConfigurationItem.hpp"
#include "cta/DriveQuota.hpp"
#include "cta/MountCriteria.hpp"

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
   * @param archivalDriveQuota The tape drive quota for the user group when
   * mounting tapes for file archival.
   * @param retrievalDriveQuota The tape drive quota for the user group when
   * mounting tapes for file retrieval.
   * @param archivalMountCriteria The criteria of the user group to be met in
   * order to justify mounting a tape for file archival.
   * @param retrievalMountCriteria The criteria of the user group to be met in
   * order to justify mounting a tape for file retrieval.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  UserGroup(
    const std::string &name,
    const DriveQuota &archivalDriveQuota,
    const DriveQuota &retrievalDriveQuota,
    const MountCriteria &archivalMountCriteria,
    const MountCriteria &retrievalMountCriteria,
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
   * file archival.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file archival.
   */
  const DriveQuota &getArchivalDriveQuota() const throw();

  /**
   * Returns the tape drive quota for the user group when mounting tapes for
   * file retrieval.
   *
   * @return The tape drive quota for the user group when mounting tapes for
   * file retrieval.
   */
  const DriveQuota &getRetrievalDriveQuota() const throw();

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file archival.
   */
  const MountCriteria &getArchivalMountCriteria() const throw();

  /**
   * Returns the criteria of the user group to be met in order to justify
   * mounting a tape for file retrieval.
   */
  const MountCriteria &getRetrievalMountCriteria() const throw();

private:

  /**
   * The name of the user group.
   */
  std::string m_name;

  /**
   * The tape drive quota for the user group when mounting tapes for file
   * archival.
   */
  DriveQuota m_archivalDriveQuota;

  /**
   * The tape drive quota for the user group when mounting tapes for file
   * retrieval.
   */
  DriveQuota m_retrievalDriveQuota;

  /**
   * The criteria of the user group to be met in order to justify mounting a
   * tape for file archival.
   */
  MountCriteria m_archivalMountCriteria;

  /**
   * The criteria of the user group to be met in order to justify mounting a
   * tape for file retrieval.
   */
  MountCriteria m_retrievalMountCriteria;

}; // class UserGroup

} // namespace cta
