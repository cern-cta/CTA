/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "SchedulerGlobalLock.hpp"
#include "GenericObject.hpp"
#include "RootEntry.hpp"
#include <google/protobuf/util/json_util.h>

namespace cta {
namespace objectstore {

SchedulerGlobalLock::SchedulerGlobalLock(const std::string& address, Backend& os) :
ObjectOps<serializers::SchedulerGlobalLock, serializers::SchedulerGlobalLock_t>(os, address) {}

SchedulerGlobalLock::SchedulerGlobalLock(GenericObject& go) :
ObjectOps<serializers::SchedulerGlobalLock, serializers::SchedulerGlobalLock_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

void SchedulerGlobalLock::initialize() {
  // Setup underlying object
  ObjectOps<serializers::SchedulerGlobalLock, serializers::SchedulerGlobalLock_t>::initialize();
  // Setup the object
  m_payload.set_nextmountid(1);
  m_payloadInterpreted = 1;
}

void SchedulerGlobalLock::garbageCollect(const std::string& presumedOwner,
                                         AgentReference& agentReference,
                                         log::LogContext& lc,
                                         cta::catalogue::Catalogue& catalogue) {
  checkPayloadWritable();
  // If the agent is not anymore the owner of the object, then only the very
  // last operation of the drive register creation failed. We have nothing to do.
  if (presumedOwner != m_header.owner()) {
    return;
  }
  // If the owner is still the agent, we have 2 possibilities:
  // 1) The register is referenced by the root entry. We just need to officialise
  // the ownership on the scheduler lock(if not, we will either get the NotAllocated
  // exception) or the address will be different.
  {
    RootEntry re(m_objectStore);
    ScopedSharedLock rel(re);
    re.fetch();
    try {
      if (re.getSchedulerGlobalLock() == getAddressIfSet()) {
        setOwner(re.getAddressIfSet());
        commit();
        return;
      }
    }
    catch (RootEntry::NotAllocated&) {
    }
  }
  // 2) The tape pool is not referenced in the root entry. We can just clean it up.
  if (!isEmpty()) {
    throw(NotEmpty("Trying to garbage collect a non-empty AgentRegister: internal error"));
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("schedulerGlobalLock", getAddressIfSet());
  lc.log(log::INFO, "In SchedulerGlobalLock::garbageCollect(): Garbage collected and removed scheduler global lock.");
}

uint64_t SchedulerGlobalLock::getIncreaseCommitMountId() {
  checkPayloadWritable();
  uint64_t ret = m_payload.nextmountid();
  m_payload.set_nextmountid(ret + 1);
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
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}

}  // namespace objectstore
}  // namespace cta
