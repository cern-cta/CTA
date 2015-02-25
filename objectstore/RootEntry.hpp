#pragma once

#include "objectstore/cta.pb.h"

#include "Backend.hpp"
#include "ObjectOps.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {

class RootEntry: public ObjectOps<serializers::RootEntry> {
public:
  // construtor
  RootEntry(Backend & os);
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & w): cta::exception::Exception(w) {}
  };
  
  // Get the name of the agent register (or exception if not available)
  std::string getAgentRegister();
  
  // Get the name of a (possibly freshly created) agent register
  std::string allocateOrGetAgentRegister(Agent & agent);
  
  // Get the name of the JobPool (or exception if not available)
  std::string getJobPool();
  
  // Get the name of a (possibly freshly created) job pool
  std::string allocateOrGetJobPool(Agent & agent);
  
  // Dump the root entry
  std::string dump ();

  private:
    static const std::string s_rootEntryName;
};

}}


