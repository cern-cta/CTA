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

#include "Backend.hpp"
#include "ObjectOps.hpp"
#include <sstream>

namespace cta { namespace objectstore {

class StorageClassList: public ObjectOps<serializers::JobPool> {
public:
  StorageClassList(const std::string & name, Backend & os):
  ObjectOps<serializers::StorageClassList>(os, name) {}
  
  void addStorageClass (
    const std::string & name,
    
    Agent & agent) {
    throw cta::exception::Exception("TODO");
  }
  
  uint32_t getStorageClassCount
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & context): cta::exception::Exception(context) {}
  };
  
  std::string dump(Agent & agent) {
    checkPayloadReadable();
    std::stringstream ret;
    ret << "<<<< StorageClassList " << getNameIfSet() << " dump start" << std::endl
        << "Migration=" << m_payload.migration() << std::endl
        << "Recall=" << m_payload.recall() << std::endl
        << "RecallCounter=" << m_payload.recallcounter() << std::endl;
    ret << ">>>> JobPool " << getNameIfSet() << " dump end" << std::endl;
    return ret.str();
  }
  
  std::string getRecallFIFO () {
    checkPayloadReadable();
    if (m_payload.recall().size())
      return m_payload.recall();
    throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
  }
  
  // Get the name of a (possibly freshly created) recall FIFO
  std::string allocateOrGetRecallFIFO(Agent & agent) {
    // Check if the job pool exists
    try {
      return getRecallFIFO();
    } catch (NotAllocatedEx &) {
      throw;
      /*// If we get here, the job pool is not created yet, so we have to do it:
      // lock the entry again, for writing
      serializers::JobPool res;
      ContextHandle & ctx = agent.getFreeContext();
      lockExclusiveAndRead(res, ctx, __func__);
      // If the registry is already defined, somebody was faster. We're done.
      if (res.recall().size()) {
        unlock(ctx);
        return res.recall();
      }
      // We will really create the register
      // decide on the object's name
      std::string FIFOName (agent.nextId("recallFIFO"));
      // Record the FIFO in the intent log
      agent.addToIntend(selfName(), FIFOName, serializers::RecallFIFO_t);
      // The potential object can now be garbage collected if we die from here.
      // Create the object, then lock. The name should be unique, so no race.
      serializers::FIFO rfs;
      rfs.set_readpointer(0);
      writeChild(FIFOName, rfs);
      // If we lived that far, we can update the jop pool to point to the FIFO
      res.set_recall(FIFOName);
      agent.removeFromIntent(selfName(), FIFOName, serializers::RecallFIFO_t);
      write(res);
      // release the lock, and return the register name
      unlock(ctx);
      return FIFOName;*/
    }
  }
  
  std::string getRecallCounter (Agent & agent) {
    checkPayloadReadable();
    if (m_payload.recallcounter().size())
      return m_payload.recallcounter();
    throw NotAllocatedEx("In RootEntry::getRecallCounter: recallCounter not yet allocated");
  }
  
  std::string allocateOrGetRecallCounter(Agent & agent) {
    // Check if the counter exists
    try {
      return getRecallCounter(agent);
    } catch (NotAllocatedEx &) {
      throw;/*
      // If we get here, the job pool is not created yet, so we have to do it:
      // lock the entry again, for writing
      serializers::JobPool res;
      ContextHandle & ctx = agent.getFreeContext();
      lockExclusiveAndRead(res, ctx, __func__);
      // If the registry is already defined, somebody was faster. We're done.
      if (res.recallcounter().size()) {
        unlock(ctx);
        return res.recallcounter();
      }
      // We will really create the register
      // decide on the object's name
      std::string recallCounterName (agent.nextId("recallCounter"));
      // Record the FIFO in the intent log
      agent.addToIntend(selfName(), recallCounterName, serializers::RecallFIFO_t);
      // The potential object can now be garbage collected if we die from here.
      // Create the object, then lock. The name should be unique, so no race.
      serializers::Counter cs;
      cs.set_count(0);
      writeChild(recallCounterName, cs);
      // If we lived that far, we can update the jop pool to point to the FIFO
      res.set_recallcounter(recallCounterName);
      agent.removeFromIntent(selfName(), recallCounterName, serializers::RecallFIFO_t);
      write(res);
      // release the lock, and return the register name
      unlock(ctx);
      return recallCounterName;*/
    }
  }
  
  // The following functions are hidden from the user in order to provide
  // higher level functionnality
  //std::string allocateOrGetMigrationFIFO
  
};

}}
