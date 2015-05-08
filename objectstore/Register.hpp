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

class Register: private ObjectOps<serializers::Register> {
public:
  Register(const std::string & name, Agent & agent):
  ObjectOps<serializers::Register>(agent.objectStore(), name) {
    // Check that the entry is present and readable (depending on implementation
    // of object store, locking might or might not succeed)
    serializers::Register rs;
    getPayloadFromObjectStoreAutoLock(rs, agent.getFreeContext());
  }
  
  void addElement (std::string name, ContextHandle & context) {
    serializers::Register rs;
    lockExclusiveAndRead(rs, context, __func__);
    rs.add_elements(name);
    write(rs);
    unlock(context);
  }
  
  void removeElement (const std::string  & name, ContextHandle & context) {
    serializers::Register rs;
    lockExclusiveAndRead(rs, context, __func__);
    bool found;
    do {
      found = false;
      for (int i=0; i<rs.mutable_elements()->size(); i++) {
        if (name == *rs.mutable_elements(i)) {
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
    serializers::Register rs;
    getPayloadFromObjectStoreAutoLock(rs, agent.getFreeContext());
    std::list<std::string> ret;
    for (int i=0; i<rs.elements_size(); i++) {
      ret.push_back(rs.elements(i));
    }
    return ret;
  }
  
  std::string dump(const std::string & title, Agent & agent) {
    serializers::Register rs;
    getPayloadFromObjectStoreAutoLock(rs, agent.getFreeContext());
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

}}