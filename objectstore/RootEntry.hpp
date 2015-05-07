/**
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


