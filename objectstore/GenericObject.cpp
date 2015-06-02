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

#include "GenericObject.hpp"

namespace cta {  namespace objectstore {

void GenericObject::fetch() {
  // Check that the object is locked, one way or another
  if(!m_locksCount)
    throw NotLocked("In ObjectOps::fetch(): object not locked");
  m_existingObject = true;
  // Get the header from the object store. We don't care for the type
  m_header.ParseFromString(m_objectStore.read(getAddressIfSet()));
  m_headerInterpreted = true;
}

serializers::ObjectType GenericObject::type() {
  checkHeaderReadable();
  return m_header.type();
}

void GenericObject::commit() {
  checkHeaderWritable();
  m_objectStore.atomicOverwrite(getAddressIfSet(), m_header.SerializeAsString());
}

void GenericObject::insert() {
  throw ForbiddenOperation("In GenericObject::insert: this operation is not possible");
}

void GenericObject::initialize() {
  throw ForbiddenOperation("In GenericObject::initialize: this operation is not possible");
}

}}
