#pragma once

#include "ObjectOps.hpp"
#include "FIFO.hpp"
#include "Agent.hpp"

namespace cta { namespace objectstore {

class JobPool: private ObjectOps<serializers::JobPool> {
public:
  JobPool(const std::string & name, Agent & agent):
  ObjectOps<serializers::JobPool>(agent.objectStore(), name) {
    serializers::JobPool jps;
    updateFromObjectStore(jps, agent.getFreeContext());
  }
  
  void PostRecallJob (const std::string & job, Agent & agent) {
    FIFO recallFIFO(allocateOrGetRecallFIFO(agent), agent);
    recallFIFO.push(job, agent);
  }
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & context): cta::exception::Exception(context) {}
  };
  
  std::string dump(Agent & agent) {
    serializers::JobPool jps;
    updateFromObjectStore(jps, agent.getFreeContext());
    std::stringstream ret;
    ret << "<<<< JobPool " << selfName() << " dump start" << std::endl
        << "Migration=" << jps.migration() << std::endl
        << "Recall=" << jps.recall() << std::endl;
    ret << ">>>> JobPool " << selfName() << " dump end" << std::endl;
    return ret.str();
  }
  
  std::string getRecallFIFO (Agent & agent) {
    // Check if the recall FIFO exists
    serializers::JobPool res;
    updateFromObjectStore(res, agent.getFreeContext());
    // If the registry is defined, return it, job done.
    if (res.recall().size())
      return res.recall();
    throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
  }
  
  // Get the name of a (possibly freshly created) recall FIFO
  std::string allocateOrGetRecallFIFO(Agent & agent) {
    // Check if the job pool exists
    try {
      return getRecallFIFO(agent);
    } catch (NotAllocatedEx &) {
      // If we get here, the job pool is not created yet, so we have to do it:
      // lock the entry again, for writing
      serializers::JobPool res;
      ContextHandle ctx = agent.getFreeContext();
      lockExclusiveAndRead(res, ctx);
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
      return FIFOName;
    }
  }
  
  // The following functions are hidden from the user in order to provide
  // higher level functionnality
  //std::string allocateOrGetMigrationFIFO
  
};

}}
