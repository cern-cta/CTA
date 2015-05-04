#include "cta/ArchivalRouteId.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRouteId::ArchivalRouteId():
  m_copyNb(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchivalRouteId::ArchivalRouteId(
  const std::string &storageClassName,
  const uint16_t copyNb):
  m_storageClassName(storageClassName),
  m_copyNb(copyNb) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::ArchivalRouteId::operator<(const ArchivalRouteId &rhs) const {
  if(m_storageClassName != rhs.m_storageClassName) {
    return m_storageClassName < rhs.m_storageClassName;
  } else {
    return m_copyNb < rhs.m_copyNb;
  }
}

//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::ArchivalRouteId::getStorageClassName() const throw() {
  return m_storageClassName;
}

//------------------------------------------------------------------------------
// getCopyNb
//------------------------------------------------------------------------------
uint16_t cta::ArchivalRouteId::getCopyNb() const throw() {
  return m_copyNb;
}
