#include "AgentRegister.hpp"

cta::objectstore::AgentRegister::AgentRegister(const std::string & name, Agent & agent):
ObjectOps<serializers::AgentRegister>(agent.objectStore(), name) {
  // Check that the entry is present and readable (depending on implementation
  // of object store, locking might or might not succeed)
  serializers::AgentRegister rs;
  updateFromObjectStore(rs);
}

void cta::objectstore::AgentRegister::addElement (std::string name, Agent & agent) {
  serializers::AgentRegister rs;
  lockExclusiveAndRead(rs);
  rs.add_elements(name);
  write(rs);
  unlock();
}

void cta::objectstore::AgentRegister::removeElement (const std::string  & name, Agent & agent) {
  serializers::AgentRegister rs;
  lockExclusiveAndRead(rs);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.mutable_elements()->size(); i++) {
      if (name == *rs.mutable_elements(i)) {
        found = true;
        rs.mutable_elements()->SwapElements(i, rs.elements_size()-1);
        rs.mutable_elements()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(rs);
  unlock();
}

void cta::objectstore::AgentRegister::addIntendedElement(std::string name, Agent& agent) {
  serializers::AgentRegister rs;
  lockExclusiveAndRead(rs);
  rs.add_intendedelements(name);
  write(rs);
  unlock();
}

void cta::objectstore::AgentRegister::upgradeIntendedElementToActual(std::string name, Agent& agent) {
  serializers::AgentRegister rs;
  lockExclusiveAndRead(rs);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.mutable_intendedelements()->size(); i++) {
      if (name == *rs.mutable_intendedelements(i)) {
        found = true;
        rs.mutable_intendedelements()->SwapElements(i, rs.intendedelements_size()-1);
        rs.mutable_intendedelements()->RemoveLast();
        break;
      }
    }
  } while (found);
  rs.add_elements(name);
  write(rs);
  unlock();
}


void cta::objectstore::AgentRegister::removeIntendedElement(const std::string& name, Agent& agent) {
  serializers::AgentRegister rs;
  lockExclusiveAndRead(rs);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.mutable_intendedelements()->size(); i++) {
      if (name == *rs.mutable_intendedelements(i)) {
        found = true;
        rs.mutable_intendedelements()->SwapElements(i, rs.intendedelements_size()-1);
        rs.mutable_intendedelements()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(rs);
  unlock();
}



std::list<std::string> cta::objectstore::AgentRegister::getElements(Agent & agent) {
  serializers::AgentRegister rs;
  updateFromObjectStore(rs);
  std::list<std::string> ret;
  for (int i=0; i<rs.elements_size(); i++) {
    ret.push_back(rs.elements(i));
  }
  return ret;
}

std::string cta::objectstore::AgentRegister::dump(Agent & agent) {
  serializers::AgentRegister rs;
  updateFromObjectStore(rs);
  std::stringstream ret;
  ret<< "<<<< AgentRegister " << selfName() << " dump start" << std::endl
    << "Array size=" << rs.elements_size() << std::endl;
  for (int i=0; i<rs.elements_size(); i++) {
    ret << "element[" << i << "]=" << rs.elements(i) << std::endl;
  }
  ret << "Intent array size=" << rs.intendedelements_size() << std::endl;
  for (int i=0; i<rs.intendedelements_size(); i++) {
    ret << "intendedElement[" << i << "]=" << rs.intendedelements(i) << std::endl;
  }
  ret<< ">>>> AgentRegister " << selfName() << " dump end" << std::endl;
  return ret.str();
}