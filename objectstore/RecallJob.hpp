#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {
  
class RecallJob: private ObjectOps<serializers::RecallJob> {
public:
  static std::string create(const std::string & container, 
    const std::string & source, const std::string & destination, Agent & agent) {
    serializers::RecallJob rjs;
    rjs.set_owner(container);
    rjs.set_source(source);
    rjs.set_destination(destination);
    std::string ret = agent.nextId("RecallJob");
    agent.addToIntend(container, ret, serializers::RecallJob_t);
    agent.objectStore().atomicOverwrite(ret, rjs.SerializeAsString());
    return ret;
  }
  
  RecallJob(const std::string & name, Agent & agent):
    ObjectOps<serializers::RecallJob>(agent.objectStore(), name){
    serializers::RecallJob rjs;
    updateFromObjectStore(rjs, agent.getFreeContext());
  }
  
  void remove() {
    ObjectOps<serializers::RecallJob>::remove();
  }
  
  std::string source(Agent & agent) {
    serializers::RecallJob rjs;
    updateFromObjectStore(rjs, agent.getFreeContext());
    return rjs.source();
  }
  
  std::string destination(Agent & agent) {
    serializers::RecallJob rjs;
    updateFromObjectStore(rjs, agent.getFreeContext());
    return rjs.destination();
  }
  
  
  std::string owner(Agent & agent) {
    serializers::RecallJob rjs;
    updateFromObjectStore(rjs, agent.getFreeContext());
    return rjs.owner();
  }
};
 
}}
