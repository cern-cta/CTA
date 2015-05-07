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
    getPayloadFromObjectStoreAutoLock(cs, agent.getFreeContext());
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
    getPayloadFromObjectStoreAutoLock(cs, agent.getFreeContext());
    return cs.count();
  }
};

}}
