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

#include "scheduler/StorageClass.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass():
  m_nbCopies(0) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::StorageClass::~StorageClass() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::StorageClass::StorageClass(
  const std::string &name,
  const uint16_t nbCopies,
  const UserIdentity &creator,
  const std::string &comment,
  const time_t creationTime):
  ConfigurationItem(creator, comment, creationTime),
  m_name(name),
  m_nbCopies(nbCopies) {
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::StorageClass::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getNbCopies
//------------------------------------------------------------------------------
uint16_t cta::StorageClass::getNbCopies() const throw() {
  return m_nbCopies;
}
