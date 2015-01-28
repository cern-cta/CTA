#pragma once

#include "ObjectOps.hpp"
#include "FIFO.hpp"
#include "Agent.hpp"

class JobPool: private ObjectOps<cta::objectstore::jobPool> {
public:
  JobPool(const std::string & name, Agent & agent):
  ObjectOps<cta::objectstore::jobPool>(agent.objectStore(), name) {
    cta::objectstore::jobPool jps;
    updateFromObjectStore(jps, agent.getFreeContext());
  }
  
  void PostRecallJob (const std::string & string, ContextHandle & context, const std::string & MigrationFIFOId) {
    cta::objectstore::jobPool jps;
    lockExclusiveAndRead(jps, context);
  }
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & context): cta::exception::Exception(context) {}
  };
  
private:
  std::string getRecallFIFO (Agent & agent) {
    // Check if the recall FIFO exists
    cta::objectstore::jobPool res;
    updateFromObjectStore(res, agent.getFreeContext());
    // If the registry is defined, return it, job done.
    if (res.recall().size())
      return res.recall();
    throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
  }
  
  // Get the name of a (possibly freshly created) recall FIFO
  std::string allocateOrGetJobPool(Agent & agent) {
    // Check if the job pool exists
    try {
      return getRecallFIFO(agent);
    } catch (NotAllocatedEx &) {
      // If we get here, the job pool is not created yet, so we have to do it:
      // lock the entry again, for writing
      cta::objectstore::jobPool res;
      ContextHandle ctx = agent.getFreeContext();
      lockExclusiveAndRead(res, ctx);
      // If the registry is already defined, somebody was faster. We're done.
      if (res.recall().size()) {
        unlock(ctx);
        return res.recall();
      }
      // We will really create the register
      // decide on the object's name
      std::string FIFOName (agent.nextId("recallFIFO-"));
      // Record the FIFO in the intent log
      agent.addToIntend(selfName(), FIFOName, "recallFIFO");
      // The potential object can now be garbage collected if we die from here.
      // Create the object, then lock. The name should be unique, so no race.
      cta::objectstore::jobPool jps;
      jps.set_migration("");
      jps.set_recall("");
      writeChild(FIFOName, jps);
      // If we lived that far, we can update the jop pool to point to the FIFO
      res.set_recall(FIFOName);
      agent.removeFromIntent(selfName(), FIFOName, "recallFIFO");
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