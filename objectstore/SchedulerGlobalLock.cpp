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

#include "SchedulerGlobalLock.hpp"
#include "GenericObject.hpp"
#include "RootEntry.hpp"
#include <json/json.h>

namespace cta { namespace objectstore { 

  
SchedulerGlobalLock::SchedulerGlobalLock(const std::string& address, Backend& os):
  ObjectOps<serializers::SchedulerGlobalLock>(os, address) { }

SchedulerGlobalLock::SchedulerGlobalLock(GenericObject& go):
  ObjectOps<serializers::SchedulerGlobalLock>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void SchedulerGlobalLock::initialize() {
  // Setup underlying object
  ObjectOps<serializers::SchedulerGlobalLock>::initialize();
  // Setup the object
  m_payload.set_nextmountid(1);
  m_payloadInterpreted = 1;
}

void SchedulerGlobalLock::garbageCollect(const std::string &presumedOwner) {
  checkPayloadWritable();
  // If the agent is not anymore the owner of the object, then only the very
  // last operation of the drive register creation failed. We have nothing to do.
  if (presumedOwner != m_header.owner())
    return;
  // If the owner is still the agent, we have 2 possibilities:
  // 1) The register is referenced by the root entry. We just need to officialise
  // the ownership on the scheduler lock(if not, we will either get the NotAllocated 
  // exception) or the address will be different.
  {
    RootEntry re(m_objectStore);
    ScopedSharedLock rel (re);
    re.fetch();
    try {
      if (re.getSchedulerGlobalLock() == getAddressIfSet()) {
        setOwner(re.getAddressIfSet());
        commit();
        return;
      }
    } catch (RootEntry::NotAllocated &) {}
  }
  // 2) The tape pool is not referenced in the root entry. We can just clean it up.
  if (!isEmpty()) {
    throw (NotEmpty("Trying to garbage collect a non-empty AgentRegister: internal error"));
  }
  remove();
}

uint64_t SchedulerGlobalLock::getIncreaseCommitMountId() {
  checkPayloadWritable();
  uint64_t ret = m_payload.nextmountid();
  m_payload.set_nextmountid(ret+1);
  return ret;
}

void SchedulerGlobalLock::setNextMountId(uint64_t nextId) {
  checkPayloadWritable();
  m_payload.set_nextmountid(nextId);
}

bool SchedulerGlobalLock::isEmpty() {
  checkPayloadReadable();
  // There is no content in the global lock.
  return true;
}

std::string SchedulerGlobalLock::dump() {  
  checkPayloadReadable();
  std::stringstream ret;
  ret << "SchedulerGlobalLock" << std::endl;
  struct json_object * jo = json_object_new_object();
  
  json_object_object_add(jo, "nextmountid", json_object_new_int64(m_payload.nextmountid()));
  
  ret << json_object_to_json_string_ext(jo, JSON_C_TO_STRING_PRETTY) << std::endl;
  return ret.str();
}
  
}}