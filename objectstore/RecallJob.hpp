/*
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
    rjs.set_status("OK");
    std::string ret = agent.nextId("RecallJob");
    agent.addToIntend(container, ret, serializers::RecallJob_t);
    agent.objectStore().create(ret, rjs.SerializeAsString());
    return ret;
  }
  
  RecallJob(const std::string & name, Agent & agent):
    ObjectOps<serializers::RecallJob>(agent.objectStore(), name){
    serializers::RecallJob rjs;
    getPayloadFromObjectStoreAutoLock(rjs, agent.getFreeContext());
  }
  
  void remove() {
    ObjectOps<serializers::RecallJob>::remove();
  }
  
  std::string source(Agent & agent) {
    serializers::RecallJob rjs;
    getPayloadFromObjectStoreAutoLock(rjs, agent.getFreeContext());
    return rjs.source();
  }
  
  std::string destination(Agent & agent) {
    serializers::RecallJob rjs;
    getPayloadFromObjectStoreAutoLock(rjs, agent.getFreeContext());
    return rjs.destination();
  }
  
  
  std::string owner(Agent & agent) {
    serializers::RecallJob rjs;
    getPayloadFromObjectStoreAutoLock(rjs, agent.getFreeContext());
    return rjs.owner();
  }
};
 
}}
