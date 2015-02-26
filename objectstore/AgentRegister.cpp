#include "AgentRegister.hpp"
#include "ProtcolBuffersAlgorithms.hpp"

cta::objectstore::AgentRegister::AgentRegister(const std::string & name, Backend & os):
ObjectOps<serializers::AgentRegister>(os, name) {}

void cta::objectstore::AgentRegister::addElement (std::string name) {
  checkPayloadWritable();
  std::string * ag = m_payload.mutable_elements()->Add();
  *ag = name;
}

void cta::objectstore::AgentRegister::removeElement (const std::string  & name) {
  checkPayloadReadable();
  serializers::removeString(m_payload.mutable_elements(), name);
}


void cta::objectstore::AgentRegister::addIntendedElement(std::string name) {
  checkPayloadWritable();
  std::string * ag = m_payload.mutable_intendedelements()->Add();
  *ag = name;
}

void cta::objectstore::AgentRegister::upgradeIntendedElementToActual(std::string name) {
  checkPayloadWritable();
  serializers::removeString(m_payload.mutable_intendedelements(), name);
  std::string * ag = m_payload.mutable_elements()->Add();
  *ag = name;
}


void cta::objectstore::AgentRegister::removeIntendedElement(const std::string& name) {
  checkPayloadWritable();
  serializers::removeString(m_payload.mutable_intendedelements(), name);
}



std::list<std::string> cta::objectstore::AgentRegister::getElements() {
  std::list<std::string> ret;
  for (int i=0; i<m_payload.elements_size(); i++) {
    ret.push_back(m_payload.elements(i));
  }
  return ret;
}

std::string cta::objectstore::AgentRegister::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret<< "<<<< AgentRegister " << getNameIfSet() << " dump start" << std::endl
    << "Array size=" << m_payload.elements_size() << std::endl;
  for (int i=0; i<m_payload.elements_size(); i++) {
    ret << "element[" << i << "]=" << m_payload.elements(i) << std::endl;
  }
  ret << "Intent array size=" << m_payload.intendedelements_size() << std::endl;
  for (int i=0; i<m_payload.intendedelements_size(); i++) {
    ret << "intendedElement[" << i << "]=" << m_payload.intendedelements(i) << std::endl;
  }
  ret<< ">>>> AgentRegister " << getNameIfSet() << " dump end" << std::endl;
  return ret.str();
}