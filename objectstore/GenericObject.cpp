#include "GenericObject.hpp"

namespace cta {  namespace objectstore {

void GenericObject::fetch() {
  // Check that the object is locked, one way or another
  if(!m_locksCount)
    throw NotLocked("In ObjectOps::fetch(): object not locked");
  m_existingObject = true;
  // Get the header from the object store. We don't care for the type
  m_header.ParseFromString(m_objectStore.read(getNameIfSet()));
  m_headerInterpreted = true;
}

serializers::ObjectType GenericObject::type() {
  checkHeaderReadable();
  return m_header.type();
}

void GenericObject::commit() {
  checkHeaderWritable();
  m_objectStore.atomicOverwrite(getNameIfSet(), m_header.SerializeAsString());
}

void GenericObject::insert() {
  throw ForbiddenOperation("In GenericObject::insert: this operation is not possible");
}

void GenericObject::initialize() {
  throw ForbiddenOperation("In GenericObject::initialize: this operation is not possible");
}

}}
