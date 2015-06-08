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
#include "AgentRegister.hpp"
#include "TapePool.hpp"

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

void GenericObject::transplantHeader(ObjectOpsBase& destination) {
  destination.m_header = m_header;
  destination.m_existingObject = m_existingObject;
  destination.m_headerInterpreted = m_headerInterpreted;
  destination.m_name = m_name;
  destination.m_nameSet = m_nameSet;
  destination.m_payloadInterpreted = false;
    }

Backend& GenericObject::objectStore() {
  return m_objectStore;
}


void GenericObject::garbageCollect(ScopedExclusiveLock& lock) {
  checkHeaderWritable();
  switch(m_header.type()) {
    case serializers::AgentRegister_t: {
      AgentRegister ar(*this);
      lock.transfer(ar);
      ar.garbageCollect();
    } break;
    case serializers::TapePool_t: {
      TapePool tp(*this);
      lock.transfer(tp);
      tp.garbageCollect();
    } break;
    default: {
      std::stringstream err;
      err << "In GenericObject::garbageCollect, unsupported type: "
          << m_header.type();
      throw UnsupportedType(err.str());
    }
  }
}


}}
