#include "ArchiveRouteId.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveRouteId::ArchiveRouteId():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveRouteId::ArchiveRouteId(
  const std::string &storageClassName,
  const uint16_t copyNb):
  m_storageClassName(storageClassName),
  m_copyNb(copyNb) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::ArchiveRouteId::operator<(const ArchiveRouteId &rhs) const {
  if(m_storageClassName != rhs.m_storageClassName) {
    return m_storageClassName < rhs.m_storageClassName;
  } else {
    return m_copyNb < rhs.m_copyNb;
  }
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchiveRouteId::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint16_t cta::ArchiveRouteId::getCopyNb() const throw() {
  return m_copyNb;
}
