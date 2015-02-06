#pragma once

#include "MigrationRoute.hpp"
#include "MigrationRouteId.hpp"

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Container of migration routes.
 */
class MigrationRoutes {
public:

  /**
   * Creates the specified migration route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param creator The identity of the user that created the storage class.
   * @param comment Comment describing the storage class.
   */
  void createMigrationRoute(
    const std::string &storageClassName,
    const uint8_t copyNb,
    const std::string &tapePoolName,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Deletes the specified migration route.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void deleteMigrationRoute(
    const std::string &storageClassName,
    const uint8_t copyNb);

  /**
   * Gets the current list of migration routes.
   *
   * @return The current list of migration routes.
   */
  std::list<MigrationRoute> getMigrationRoutes() const;

  /**
   * Throws an exception if the specified migration route does not exist.
   *
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  void checkMigrationRouteExists(const std::string &storageClassName,
    const uint8_t copyNb) const;

  /**
   * Returns true if the specified tape pool is in one of the current migration
   * routes.
   *
   * @param name The name of the tape pool.
   */
  bool tapePoolIsInAMigrationRoute(const std::string &name) const;

private:

  /**
   * The current mapping from migration-route identifier to migration route.
   */
  std::map<MigrationRouteId, MigrationRoute> m_migrationRoutes;

  /**
   * Throws an exception if the specified migration route already exists.
   *
   * @param routeId The identity of the migration route.
   */
  void checkMigrationRouteDoesNotAlreadyExists(const MigrationRouteId &routeId)
    const;

}; // class MigrationRoutes

} // namespace cta
