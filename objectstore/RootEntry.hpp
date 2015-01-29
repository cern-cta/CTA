#pragma once

#include "objectstore/cta.pb.h"

#include "ObjectStores.hpp"
#include "ObjectOps.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {

class RootEntry: private ObjectOps<serializers::RootEntry> {
public:
  // Initializer.
  static void init(ObjectStore & os);
  
  // construtor, when the backend store exists.
  // Checks the existence and correctness of the root entry
  RootEntry(Agent & agent);
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & w): cta::exception::Exception(w) {}
  };
  
  // Get the name of the agent register (or exception if not available)
  std::string getAgentRegister(Agent & agent);
  
  // Get the name of a (possibly freshly created) agent register
  std::string allocateOrGetAgentRegister(Agent & agent);
  
  // Get the name of the JobPool (or exception if not available)
  std::string getJobPool(Agent & agent);
  
  // Get the name of a (possibly freshly created) job pool
  std::string allocateOrGetJobPool(Agent & agent);
  
  // Dump the root entry
  std::string dump (Agent & agent);

  private:
    static const std::string s_rootEntryName;
};

}}


