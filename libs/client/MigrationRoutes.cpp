#include "Exception.hpp"
#include "MigrationRoutes.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createMigrationRoute
//------------------------------------------------------------------------------
void cta::MigrationRoutes::createMigrationRoute(
  const std::string &storageClassName,
  const uint8_t copyNb,
  const std::string &tapePoolName,
  const UserIdentity &creator,
  const std::string &comment) {
  const MigrationRouteId routeId(storageClassName, copyNb);
  checkMigrationRouteDoesNotAlreadyExists(routeId);
  MigrationRoute route(
    storageClassName,
    copyNb,
    tapePoolName,
    creator,
    comment);
  m_migrationRoutes[routeId] = route;
}

//------------------------------------------------------------------------------
// checkMigrationRouteDoesNotAlreadyExists
//------------------------------------------------------------------------------
void cta::MigrationRoutes::checkMigrationRouteDoesNotAlreadyExists(
  const MigrationRouteId &routeId) const {
  std::map<MigrationRouteId, MigrationRoute>::const_iterator itor =
    m_migrationRoutes.find(routeId);
  if(itor != m_migrationRoutes.end()) {
    std::ostringstream message;
    message << "A migration route for storage class " <<
      routeId.getStorageClassName() << " and copy number " <<
      routeId.getCopyNb() << " already exists";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// deleteMigrationRoute
//------------------------------------------------------------------------------
void cta::MigrationRoutes::deleteMigrationRoute(
  const std::string &storageClassName,
  const uint8_t copyNb) {
  const MigrationRouteId routeId(storageClassName, copyNb);
  std::map<MigrationRouteId, MigrationRoute>::iterator itor =
    m_migrationRoutes.find(routeId);
  if(itor == m_migrationRoutes.end()) {
    std::ostringstream message;
    message << "A migration route for storage class " <<
      routeId.getStorageClassName() << " and copy number " <<
      routeId.getCopyNb() << " does not exist";
    throw Exception(message.str());
  }
  m_migrationRoutes.erase(routeId);
}

//------------------------------------------------------------------------------
// getMigrationRoutes
//------------------------------------------------------------------------------
std::list<cta::MigrationRoute> cta::MigrationRoutes::getMigrationRoutes()
  const {
  std::list<cta::MigrationRoute> routes;
  for(std::map<MigrationRouteId, MigrationRoute>::const_iterator itor =
    m_migrationRoutes.begin(); itor != m_migrationRoutes.end(); itor++) {
    routes.push_back(itor->second);
  }
  return routes;
}

//------------------------------------------------------------------------------
// checkMigrationRouteExists
//------------------------------------------------------------------------------
void cta::MigrationRoutes::checkMigrationRouteExists(
  const std::string &storageClassName, const uint8_t copyNb) const {
}
