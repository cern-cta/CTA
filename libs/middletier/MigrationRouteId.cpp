#include "MigrationRouteId.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MigrationRouteId::MigrationRouteId():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MigrationRouteId::MigrationRouteId(
  const std::string &storageClassName,
  const uint8_t copyNb):
  m_storageClassName(storageClassName),
  m_copyNb(copyNb) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::MigrationRouteId::operator<(const MigrationRouteId &rhs) const {
  if(m_storageClassName != rhs.m_storageClassName) {
    return m_storageClassName < rhs.m_storageClassName;
  } else {
    return m_copyNb < rhs.m_copyNb;
  }
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::MigrationRouteId::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint8_t cta::MigrationRouteId::getCopyNb() const throw() {
  return m_copyNb;
}
