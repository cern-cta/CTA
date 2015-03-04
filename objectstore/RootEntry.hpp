#pragma once

#include "objectstore/cta.pb.h"

#include "Backend.hpp"
#include "ObjectOps.hpp"

namespace cta { namespace objectstore {

class Agent;
  
class RootEntry: public ObjectOps<serializers::RootEntry> {
public:
  // construtor
  RootEntry(Backend & os);
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & w): cta::exception::Exception(w) {}
  };
  
  // In memory initialiser
  void initialize();
  
  // Get the name of the agent register (or exception if not available)
  std::string getAgentRegister();
  
  // Get the name of a (possibly freshly created) agent register
  std::string allocateOrGetAgentRegister(Agent & agent);
  
  // Get the name of the JobPool (or exception if not available)
  std::string getJobPool();
  
  // Get the name of a (possibly freshly created) job pool
  std::string allocateOrGetJobPool(Agent & agent);
  
  // Get the name of the AdminUsersList (or exception if not available)
  std::string getAdminUsersList();
  
  // Get the name of a (possibly freshly created) AdminUsersList
  std::string allocateOrGetAdminUsersList(Agent & agent);
  
private:
  void addIntendedAgentRegistry(const std::string & name);
  
  void deleteFromIntendedAgentRegistry(const std::string & name);
  
  void setAgentRegister(const std::string & name);
  
  void addIntendedJobPool(const std::string & name);
  
  void deleteFromIntendedJobPool(const std::string & name);
  
  void setJobPool(const std::string & name);
  
public:
  // Dump the root entry
  std::string dump ();

  private:
    static const std::string s_rootEntryName;
};

}}


