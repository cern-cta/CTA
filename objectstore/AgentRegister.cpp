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

#include "AgentRegister.hpp"
#include "ProtcolBuffersAlgorithms.hpp"
#include "GenericObject.hpp"

cta::objectstore::AgentRegister::AgentRegister(Backend & os):
ObjectOps<serializers::AgentRegister>(os) {}

cta::objectstore::AgentRegister::AgentRegister(GenericObject& go): 
  ObjectOps<serializers::AgentRegister>(go.objectStore()) {
  // Here we transplant the generic object into the new object
  go.transplantHeader(*this);
  // And interpret the header.
  getPayloadFromHeader();
}

cta::objectstore::AgentRegister::AgentRegister(const std::string & name, Backend & os):
ObjectOps<serializers::AgentRegister>(os, name) {}

void cta::objectstore::AgentRegister::initialize() {
  ObjectOps<serializers::AgentRegister>::initialize();
  // There is nothing to do for the payload.
  m_payloadInterpreted = true;
}

void cta::objectstore::AgentRegister::garbageCollect() {
  checkPayloadWritable();
  if (!isEmpty()) {
    throw (NotEmpty("Trying to garbage collect a non-empty AgentRegister: internal error"));
  }
  remove();
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
  std::stringstream ret;
  ret<< "<<<< AgentRegister " << getAddressIfSet() << " dump start" << std::endl
    << "Agents array size=" << m_payload.agents_size() << std::endl;
  for (int i=0; i<m_payload.agents_size(); i++) {
    ret << "element[" << i << "]=" << m_payload.agents(i) << std::endl;
  }
  ret << "Untracked agents array size=" << m_payload.untrackedagents_size() << std::endl;
  for (int i=0; i<m_payload.untrackedagents_size(); i++) {
    ret << "intendedElement[" << i << "]=" << m_payload.untrackedagents(i) << std::endl;
  }
  ret<< ">>>> AgentRegister " << getAddressIfSet() << " dump end" << std::endl;
  return ret.str();
}