#include "Exception.hpp"
#include "MockMigrationRouteTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createMigrationRoute
//------------------------------------------------------------------------------
void cta::MockMigrationRouteTable::createMigrationRoute(
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
    time(NULL),
    comment);
  m_migrationRoutes[routeId] = route;
}

//------------------------------------------------------------------------------
// checkMigrationRouteDoesNotAlreadyExists
//------------------------------------------------------------------------------
void cta::MockMigrationRouteTable::checkMigrationRouteDoesNotAlreadyExists(
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
void cta::MockMigrationRouteTable::deleteMigrationRoute(
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
std::list<cta::MigrationRoute> cta::MockMigrationRouteTable::
  getMigrationRoutes() const {
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
void cta::MockMigrationRouteTable::checkMigrationRouteExists(
  const std::string &storageClassName, const uint8_t copyNb) const {
}

//------------------------------------------------------------------------------
// tapePoolIsInAMigrationRoute
//------------------------------------------------------------------------------
bool cta::MockMigrationRouteTable::tapePoolIsInAMigrationRoute(
  const std::string &name) const {
  for(std::map<MigrationRouteId, MigrationRoute>::const_iterator itor =
    m_migrationRoutes.begin(); itor != m_migrationRoutes.end(); itor++) {
    if(name == itor->second.getTapePoolName()) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// storageClassIsInAMigrationRoute
//------------------------------------------------------------------------------
bool cta::MockMigrationRouteTable::storageClassIsInAMigrationRoute(
  const std::string &name) const {
  for(std::map<MigrationRouteId, MigrationRoute>::const_iterator itor =
    m_migrationRoutes.begin(); itor != m_migrationRoutes.end(); itor++) {
    if(name == itor->second.getStorageClassName()) {
      return true;
    }
  }
  return false;
}
