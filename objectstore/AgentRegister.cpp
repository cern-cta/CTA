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

#include "AgentRegister.hpp"
#include "ProtocolBuffersAlgorithms.hpp"
#include "GenericObject.hpp"
#include <google/protobuf/util/json_util.h>

cta::objectstore::AgentRegister::AgentRegister(Backend & os):
ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t>(os) {}

cta::objectstore::AgentRegister::AgentRegister(GenericObject& go): 
  ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

cta::objectstore::AgentRegister::AgentRegister(const std::string & name, Backend & os):
ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t>(os, name) {}

void cta::objectstore::AgentRegister::initialize() {
  ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t>::initialize();
  // There is nothing to do for the payload.
  m_payloadInterpreted = true;
}

void cta::objectstore::AgentRegister::garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw NotEmpty("Trying to garbage collect a non-empty AgentRegister: internal error");
  }
  remove();
  log::ScopedParamContainer params(lc);
  params.add("agentRegisterObject", getAddressIfSet());
  lc.log(log::INFO, "In AgentRegister::garbageCollect(): Garbage collected and removed agent register object.");
}


bool cta::objectstore::AgentRegister::isEmpty() {
  checkPayloadReadable();
  if (m_payload.untrackedagents_size())
    return false;
  if (m_payload.agents_size())
    return false;
  return true;
}


void cta::objectstore::AgentRegister::addAgent (std::string name) {
  checkPayloadWritable();
  m_payload.add_agents(name);
  m_payload.add_untrackedagents(name);
}

void cta::objectstore::AgentRegister::removeAgent (const std::string  & name) {
  checkPayloadReadable();
  serializers::removeString(m_payload.mutable_agents(), name);
  serializers::removeString(m_payload.mutable_untrackedagents(), name);
}


void cta::objectstore::AgentRegister::trackAgent(std::string name) {
  checkPayloadWritable();
  // Check that the agent is present (next statement throws an exception
  // if the agent is not known)
  serializers::findString(m_payload.mutable_agents(), name);
  serializers::removeString(m_payload.mutable_untrackedagents(), name);
}

void cta::objectstore::AgentRegister::untrackAgent(std::string name) {
  checkPayloadWritable();
  // Check that the agent is present (next statement throws an exception
  // if the agent is not known)
  serializers::findString(m_payload.mutable_agents(), name);
  // Check that the agent is not already in the list to prevent double
  // inserts
  try {
    serializers::findString(m_payload.mutable_untrackedagents(), name);
  } catch (serializers::NotFound &) {
    m_payload.add_untrackedagents(name);
  }
}

std::list<std::string> cta::objectstore::AgentRegister::getAgents() {
  std::list<std::string> ret;
  for (int i=0; i<m_payload.agents_size(); i++) {
    ret.push_back(m_payload.agents(i));
  }
  return ret;
}

std::list<std::string> cta::objectstore::AgentRegister::getUntrackedAgents() {
  std::list<std::string> ret;
  for (int i=0; i<m_payload.untrackedagents_size(); i++) {
    ret.push_back(m_payload.untrackedagents(i));
  }
  return ret;
}

std::string cta::objectstore::AgentRegister::dump() {
  checkPayloadReadable();
  google::protobuf::util::JsonPrintOptions options;
  options.add_whitespace = true;
  options.always_print_primitive_fields = true;
  std::string headerDump;
  google::protobuf::util::MessageToJsonString(m_payload, &headerDump, options);
  return headerDump;
}
