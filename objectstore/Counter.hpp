#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {

  
class Counter: private ObjectOps<serializers::Counter> {
public:
  Counter(const std::string & name, Agent & agent):
  ObjectOps<serializers::Counter>(agent.objectStore(), name)
  {
    // check the presence of the entry
    serializers::Counter cs;
    updateFromObjectStore(cs, agent.getFreeContext());
  }
  
  void inc(Agent & agent) {
    serializers::Counter cs;
    ContextHandle & ctx = agent.getFreeContext();
    lockExclusiveAndRead(cs, ctx, __func__);
    cs.set_count(cs.count()+1);
    write(cs);
    unlock(ctx, __func__);
  }
  
  uint64_t get(Agent & agent) {
    serializers::Counter cs;
    updateFromObjectStore(cs, agent.getFreeContext());
    return cs.count();
  }
};

}}
