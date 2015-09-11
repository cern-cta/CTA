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

#include "common/TapePool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool():
  nbPartialTapes(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::TapePool::~TapePool() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool(
    const std::string &name,
    const uint32_t nbPartialTapes,
    const MountCriteriaByDirection & mountCriteriaByDirection,
    const CreationLog &creationLog):
  name(name),
  nbPartialTapes(nbPartialTapes),
  mountCriteriaByDirection(mountCriteriaByDirection),
  creationLog(creationLog) {}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::TapePool::operator<(const TapePool &rhs) const throw() {
  return name < rhs.name;
}
