#include "Exception.hpp"
#include "MockArchiveRouteTable.hpp"

#include <sstream>

//------------------------------------------------------------------------------
// createArchiveRoute
//------------------------------------------------------------------------------
void cta::MockArchiveRouteTable::createArchiveRoute(
  const std::string &storageClassName,
  const uint8_t copyNb,
  const std::string &tapePoolName,
  const UserIdentity &creator,
  const std::string &comment) {
  const ArchiveRouteId routeId(storageClassName, copyNb);
  checkArchiveRouteDoesNotAlreadyExists(routeId);
  ArchiveRoute route(
    storageClassName,
    copyNb,
    tapePoolName,
    creator, 
    time(NULL),
    comment);
  m_archiveRoutes[routeId] = route;
}

//------------------------------------------------------------------------------
// checkArchiveRouteDoesNotAlreadyExists
//------------------------------------------------------------------------------
void cta::MockArchiveRouteTable::checkArchiveRouteDoesNotAlreadyExists(
  const ArchiveRouteId &routeId) const {
  std::map<ArchiveRouteId, ArchiveRoute>::const_iterator itor =
    m_archiveRoutes.find(routeId);
  if(itor != m_archiveRoutes.end()) {
    std::ostringstream message;
    message << "A archive route for storage class " <<
      routeId.getStorageClassName() << " and copy number " <<
      routeId.getCopyNb() << " already exists";
    throw Exception(message.str());
  }
}

//------------------------------------------------------------------------------
// deleteArchiveRoute
//------------------------------------------------------------------------------
void cta::MockArchiveRouteTable::deleteArchiveRoute(
  const std::string &storageClassName,
  const uint8_t copyNb) {
  const ArchiveRouteId routeId(storageClassName, copyNb);
  std::map<ArchiveRouteId, ArchiveRoute>::iterator itor =
    m_archiveRoutes.find(routeId);
  if(itor == m_archiveRoutes.end()) {
    std::ostringstream message;
    message << "A archive route for storage class " <<
      routeId.getStorageClassName() << " and copy number " <<
      routeId.getCopyNb() << " does not exist";
    throw Exception(message.str());
  }
  m_archiveRoutes.erase(routeId);
}

//------------------------------------------------------------------------------
// getArchiveRoutes
//------------------------------------------------------------------------------
std::list<cta::ArchiveRoute> cta::MockArchiveRouteTable::
  getArchiveRoutes() const {
  std::list<cta::ArchiveRoute> routes;
  for(std::map<ArchiveRouteId, ArchiveRoute>::const_iterator itor =
    m_archiveRoutes.begin(); itor != m_archiveRoutes.end(); itor++) {
    routes.push_back(itor->second);
  }
  return routes;
}

//------------------------------------------------------------------------------
// getArchiveRoute
//------------------------------------------------------------------------------
const cta::ArchiveRoute &cta::MockArchiveRouteTable::getArchiveRoute(
  const std::string &storageClassName,
  const uint8_t copyNb) const {
  const ArchiveRouteId routeId(storageClassName, copyNb);
  std::map<ArchiveRouteId, ArchiveRoute>::const_iterator itor =
    m_archiveRoutes.find(routeId);
  if(itor == m_archiveRoutes.end()) {
    std::ostringstream message;
    message << "No archive route for storage class " << storageClassName <<
      " copy number " << copyNb;
    throw Exception(message.str());
  }
  return itor->second;
}

//------------------------------------------------------------------------------
// checkArchiveRouteExists
//------------------------------------------------------------------------------
void cta::MockArchiveRouteTable::checkArchiveRouteExists(
  const std::string &storageClassName, const uint8_t copyNb) const {
}

//------------------------------------------------------------------------------
// tapePoolIsInAArchiveRoute
//------------------------------------------------------------------------------
bool cta::MockArchiveRouteTable::tapePoolIsInAArchiveRoute(
  const std::string &name) const {
  for(std::map<ArchiveRouteId, ArchiveRoute>::const_iterator itor =
    m_archiveRoutes.begin(); itor != m_archiveRoutes.end(); itor++) {
    if(name == itor->second.getTapePoolName()) {
      return true;
    }
  }
  return false;
}

//------------------------------------------------------------------------------
// storageClassIsInAArchiveRoute
//------------------------------------------------------------------------------
bool cta::MockArchiveRouteTable::storageClassIsInAArchiveRoute(
  const std::string &name) const {
  for(std::map<ArchiveRouteId, ArchiveRoute>::const_iterator itor =
    m_archiveRoutes.begin(); itor != m_archiveRoutes.end(); itor++) {
    if(name == itor->second.getStorageClassName()) {
      return true;
    }
  }
  return false;
}
