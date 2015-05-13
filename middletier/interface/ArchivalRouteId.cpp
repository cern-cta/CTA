/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "middletier/interface/ArchivalRouteId.hpp"

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
