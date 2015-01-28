#pragma once

#include "ObjectOps.hpp"
#include <algorithm>

class Register: private ObjectOps<cta::objectstore::Register> {
public:
  Register(const std::string & name, Agent & agent):
  ObjectOps<cta::objectstore::Register>(agent.objectStore(), name) {
    // Check that the entry is present and readable (depending on implementation
    // of object store, locking might or might not succeed)
    cta::objectstore::Register rs;
    updateFromObjectStore(rs, agent.getFreeContext());
  }
  
  void addElement (std::string name, ContextHandle & context) {
    cta::objectstore::Register rs;
    lockExclusiveAndRead(rs, context);
    rs.add_elements(name);
    write(rs);
    unlock(context);
  }
  
  void removeElement (const std::string  & name, ContextHandle & context) {
    cta::objectstore::Register rs;
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
  
  std::list<std::string> getElements(Agent & agent) {
    cta::objectstore::Register rs;
    updateFromObjectStore(rs, agent.getFreeContext());
    std::list<std::string> ret;
    for (int i=0; i<rs.elements_size(); i++) {
      ret.push_back(rs.elements(i));
    }
    return ret;
  }
  
  std::string dump(const std::string & title, Agent & agent) {
    cta::objectstore::Register rs;
    updateFromObjectStore(rs, agent.getFreeContext());
    std::stringstream ret;
    ret<< "<<<< Register " << title << " dump start" << std::endl
      << "Array size=" << rs.elements_size() << std::endl;
    for (int i=0; i<rs.elements_size(); i++) {
      ret << "element[" << i << "]=" << rs.elements(i) << std::endl;
    }
    ret<< ">>>> Register " << title << " dump end" << std::endl;
    return ret.str();
  }
};