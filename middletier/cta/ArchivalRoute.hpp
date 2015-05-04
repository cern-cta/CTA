#pragma once

#include "cta/ConfigurationItem.hpp"
#include "cta/UserIdentity.hpp"

#include <stdint.h>
#include <string>
#include <time.h>

namespace cta {

/**
 * An archival route.
 */
class ArchivalRoute: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  ArchivalRoute();

  /**
   * Destructor.
   */
  ~ArchivalRoute() throw();

  /**
   * Constructor.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.  Copy numbers start from 1.
   * @param tapePoolName The name of the destination tape pool.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment Comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  ArchivalRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the storage class that identifies the source disk
   * files.
   *
   * @return The name of the storage class that identifies the source disk
   * files.
   */
  const std::string &getStorageClassName() const throw();

  /**
   * Returns the tape copy number.
   *
   * @return The tape copy number.
   */
  uint16_t getCopyNb() const throw();

  /**
   * Returns the name of the destination tape pool.
   *
   * @return The name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

private:

  /**
   * The name of the storage class that identifies the source disk files.
   */
  std::string m_storageClassName;

  /**
   * The tape copy number.
   */
  uint32_t m_copyNb;

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

}; // class ArchivalRoute

} // namespace cta
