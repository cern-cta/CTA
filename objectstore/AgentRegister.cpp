#include "AgentRegister.hpp"

AgentRegister::AgentRegister(const std::string & name, Agent & agent):
ObjectOps<cta::objectstore::AgentRegister>(agent.objectStore(), name) {
  // Check that the entry is present and readable (depending on implementation
  // of object store, locking might or might not succeed)
  cta::objectstore::AgentRegister rs;
  updateFromObjectStore(rs, agent.getFreeContext());
}

void AgentRegister::addElement (std::string name, Agent & agent) {
  cta::objectstore::AgentRegister rs;
  ContextHandle & context = agent.getFreeContext();
  lockExclusiveAndRead(rs, context);
  rs.add_elements(name);
  write(rs);
  unlock(context);
}

void AgentRegister::removeElement (const std::string  & name, Agent & agent) {
  cta::objectstore::AgentRegister rs;
  ContextHandle & context = agent.getFreeContext();
  lockExclusiveAndRead(rs, context);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.elements_size(); i++) {
      if (name == rs.elements(i)) {
        found = true;
        rs.mutable_elements()->SwapElements(i, rs.elements_size()-1);
        rs.mutable_elements()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(rs);
  unlock(context);
}

void AgentRegister::addIntendedElement(std::string name, Agent& agent) {
  cta::objectstore::AgentRegister rs;
  ContextHandle & context = agent.getFreeContext();
  lockExclusiveAndRead(rs, context);
  rs.add_intendedelements(name);
  write(rs);
  unlock(context);
}

void AgentRegister::upgradeIntendedElementToActual(std::string name, Agent& agent) {
  cta::objectstore::AgentRegister rs;
  ContextHandle & context = agent.getFreeContext();
  lockExclusiveAndRead(rs, context);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.intendedelements_size(); i++) {
      if (name == rs.intendedelements(i)) {
        found = true;
        rs.mutable_intendedelements()->SwapElements(i, rs.intendedelements_size()-1);
        rs.mutable_intendedelements()->RemoveLast();
        break;
      }
    }
  } while (found);
  rs.add_elements(name);
  write(rs);
  unlock(context);
}


void AgentRegister::removeIntendedElement(const std::string& name, Agent& agent) {
  cta::objectstore::AgentRegister rs;
  ContextHandle & context = agent.getFreeContext();
  lockExclusiveAndRead(rs, context);
  bool found;
  do {
    found = false;
    for (int i=0; i<rs.intendedelements_size(); i++) {
      if (name == rs.intendedelements(i)) {
        found = true;
        rs.mutable_intendedelements()->SwapElements(i, rs.intendedelements_size()-1);
        rs.mutable_intendedelements()->RemoveLast();
        break;
      }
    }
  } while (found);
  write(rs);
  unlock(context);
}



std::list<std::string> AgentRegister::getElements(Agent & agent) {
  cta::objectstore::AgentRegister rs;
  updateFromObjectStore(rs, agent.getFreeContext());
  std::list<std::string> ret;
  for (int i=0; i<rs.elements_size(); i++) {
    ret.push_back(rs.elements(i));
  }
  return ret;
}

std::string AgentRegister::dump(Agent & agent) {
  cta::objectstore::AgentRegister rs;
  updateFromObjectStore(rs, agent.getFreeContext());
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