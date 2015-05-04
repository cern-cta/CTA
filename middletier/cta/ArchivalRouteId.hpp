#pragma once

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class used to identify an archival route.
 */
class ArchivalRouteId {
public:

  /**
   * Constructor.
   */
  ArchivalRouteId();

  /**
   * Constructor.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  ArchivalRouteId(
    const std::string &storageClassName,
    const uint16_t copyNb);

  /**
   * Less than operator.
   *
   * @param rhs The object on the right hand side of the operator.
   */
  bool operator<(const ArchivalRouteId &rhs) const;

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

private:

  /**
   * The name of the storage class that identifies the source disk files.
   */
  std::string m_storageClassName;

  /**
   * The tape copy number.
   */
  uint16_t m_copyNb;

}; // class ArchivalRouteId

} // namespace cta
