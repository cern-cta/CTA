#pragma once

#include "Backend.hpp"
#include "ObjectOps.hpp"

namespace cta { namespace objectstore {

class JobPool: public ObjectOps<serializers::JobPool> {
public:
  JobPool(const std::string & name, Backend & os):
  ObjectOps<serializers::JobPool>(os, name) {}
  
  void PostRecallJob (const std::string & job, Agent & agent) {
    /*FIFO recallFIFO(allocateOrGetRecallFIFO(agent), agent);
    recallFIFO.push(job, agent);*/
  }
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & context): cta::exception::Exception(context) {}
  };
  
  std::string dump(Agent & agent) {
    checkPayloadReadable();
    std::stringstream ret;
    ret << "<<<< JobPool " << getNameIfSet() << " dump start" << std::endl
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
