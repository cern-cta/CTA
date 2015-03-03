#pragma once

#include "ArchiveRoute.hpp"
#include "ArchiveRouteId.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Mock database-table of archive routes.
 */
class MockArchiveRouteTable {
public:

  /**
   * Creates the specified archive route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param creator The identity of the user that created the storage class.
   * @param comment Comment describing the storage class.
   */
  void createArchiveRoute(
    const std::string &storageClassName,
    const uint16_t copyNb,
    const std::string &tapePoolName,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Deletes the specified archive route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteArchiveRoute(
    const std::string &storageClassName,
    const uint16_t copyNb);

  /**
   * Gets the current list of archive routes.
   *
   * @return The current list of archive routes.
   */
  std::list<ArchiveRoute> getArchiveRoutes() const;

  /**
   * Returns the specified archive route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @return The specified archive route.
   */
  const ArchiveRoute &getArchiveRoute(
    const std::string &storageClassName,
    const uint16_t copyNb) const;

  /**
   * Throws an exception if the specified archive route does not exist.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void checkArchiveRouteExists(const std::string &storageClassName,
    const uint16_t copyNb) const;

  /**
   * Returns true if the specified tape pool is in one of the current archive
   * routes.
   *
   * @param name The name of the tape pool.
   */
  bool tapePoolIsInAArchiveRoute(const std::string &name) const;

  /**
   * Returns true if the specified storage class is in one of the current
   * archive routes.
   *
   * @param name The name of the storage class.
   */
  bool storageClassIsInAArchiveRoute(const std::string &name) const;

private:

  /**
   * The current mapping from archive-route identifier to archive route.
   */
  std::map<ArchiveRouteId, ArchiveRoute> m_archiveRoutes;

  /**
   * Throws an exception if the specified archive route already exists.
   *
   * @param routeId The identity of the archive route.
   */
  void checkArchiveRouteDoesNotAlreadyExists(const ArchiveRouteId &routeId)
    const;

}; // class MockArchiveRouteTable

} // namespace cta
