#pragma once

#include <google/protobuf/text_format.h>
#include "objectstore/cta.pb.h"

#include "ObjectStores.hpp"
#include "ObjectOps.hpp"

class RootEntry: private ObjectOps<cta::objectstore::RootEntry> {
public:
  // Initializer.
  static void init(ObjectStore & os) {
    // check existence of root entry before creating it. We expect read to fail.
    try {
      os.read(s_rootEntryName);
      throw cta::exception::Exception("In RootEntry::init: root entry already exists");
    } catch (...) {}
    cta::objectstore::RootEntry res;
    os.atomicOverwrite(s_rootEntryName, res.SerializeAsString());
  }
  // construtor, when the backend store exists.
  // Checks the existence and correctness of the root entry
  RootEntry(ObjectStore & os, ContextHandle & context):
    ObjectOps<cta::objectstore::RootEntry>(os, s_rootEntryName) {
    // Check that the root entry is readable.
    cta::objectstore::RootEntry res;
    updateFromObjectStore(res, context);
  }
  
  class NotAllocatedEx: public cta::exception::Exception {
  public:
    NotAllocatedEx(const std::string & context): cta::exception::Exception(context) {}
  };
  
  // Get the name of the agent register (or exception if not available)
  std::string getAgentRegister(ContextHandle & context) {
    // Check if the agent register exists
    cta::objectstore::RootEntry res;
    updateFromObjectStore(res, context);
    // If the registry is defined, return it, job done.
    if (res.agentregister().size())
      return res.agentregister();
    throw NotAllocatedEx("In RootEntry::getAgentRegister: agentRegister not yet allocated");
  }
  
  // Get the name of a (possibly freshly created) agent register
  std::string allocateOrGetAgentRegister(ContextHandle & context, std::string agentRegistryId) {
    // Check if the agent register exists
    try {
      return getAgentRegister(context);
    } catch (NotAllocatedEx &) {
      // If we get here, the agent register is not created yet, so we have to do it:
      // lock the entry again, for writing
      cta::objectstore::RootEntry res;
      lockExclusiveAndRead(res, context);
      // If the registry is already defined, somebody was faster. We're done.
      if (res.agentregister().size()) {
        unlock(context);
        return res.agentregister();
      }
      // We will really create the register
      // decide on the object's name
      std::string arName ("agentRegister-");
      arName += agentRegistryId;
      // Record the agent in the intent log
      res.add_agentregisterintentlog(arName);
      // Commit the intents
      write(res);
      // The potential object can now be garbage collected if we die from here.
      // Create the object, then lock. The name should be unique, so no race.
      cta::objectstore::Register ars;
      writeChild(arName, ars);
      // If we lived that far, we can update the root entry to point to our
      // new agent register, and remove the name from the intent log.
      res.set_agentregister(arName);
      res.mutable_agentregisterintentlog()->RemoveLast();
      write(res);
      // release the lock, and return the register name
      unlock(context);
      return arName;
    }
  }
  
  // Get the name of the JobPool (or exception if not available)
  std::string getJobPool(ContextHandle & context) {
    // Check if the job pool exists
    cta::objectstore::RootEntry res;
    updateFromObjectStore(res, context);
    // If the registry is defined, return it, job done.
    if (res.jobpool().size())
      return res.jobpool();
    throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
  }
  
  // Get the name of a (possibly freshly created) job pool
  std::string allocateOrGetJobPool(ContextHandle & context, std::string jobPoolId) {
    // Check if the job pool exists
    try {
      return getJobPool(context);
    } catch (NotAllocatedEx &) {
      // If we get here, the job pool is not created yet, so we have to do it:
      // lock the entry again, for writing
      cta::objectstore::RootEntry res;
      lockExclusiveAndRead(res, context);
      // If the registry is already defined, somebody was faster. We're done.
      if (res.jobpool().size()) {
        unlock(context);
        return res.jobpool();
      }
      // We will really create the register
      // decide on the object's name
      std::string jpName ("jobPool-");
      jpName += jobPoolId;
      // Record the agent in the intent log
      res.add_jobpoolintentlog(jpName);
      // Commit the intents
      write(res);
      // The potential object can now be garbage collected if we die from here.
      // Create the object, then lock. The name should be unique, so no race.
      cta::objectstore::jobPool jps;
      jps.set_migration("");
      jps.set_recall("");
      writeChild(jpName, jps);
      // If we lived that far, we can update the root entry to point to our
      // new agent register, and remove the name from the intent log.
      res.set_jobpool(jpName);
      res.mutable_jobpoolintentlog()->RemoveLast();
      write(res);
      // release the lock, and return the register name
      unlock(context);
      return jpName;
    }
  }
  
  // Dump the root entry
  std::string dump (ContextHandle & context) {
    std::stringstream ret;
    cta::objectstore::RootEntry res;
    updateFromObjectStore(res, context);
    ret << "<<<< Root entry dump start" << std::endl;
    if (res.has_agentregister()) ret << "agentRegister=" << res.agentregister() << std::endl;
    for (int i=0; i<res.agentregisterintentlog_size(); i++) {
      ret << "agentRegisterIntentLog=" << res.agentregisterintentlog(i) << std::endl;
    }
    if (res.has_jobpool()) ret << "jobPool=" << res.jobpool() << std::endl;
    for (int i=0; i<res.jobpoolintentlog_size(); i++) {
      ret << "jobPoolIntentLog=" << res.jobpoolintentlog(i) << std::endl;
    }
    if (res.has_driveregister()) ret << "driveRegister=" << res.driveregister() << std::endl;
    for (int i=0; i<res.driveregisterintentlog_size(); i++) {
      ret << "driveRegisterIntentLog=" << res.driveregisterintentlog(i) << std::endl;
    }
    if (res.has_taperegister()) ret << "tapeRegister=" << res.taperegister() << std::endl;
    for (int i=0; i<res.taperegisterintentlog_size(); i++) {
      ret << "tapeRegisterIntentLog=" << res.taperegisterintentlog(i) << std::endl;
    }
    ret << ">>>> Root entry dump start" << std::endl;
    return ret.str();
  }

  private:
    static const std::string s_rootEntryName;
};

const std::string RootEntry::s_rootEntryName("root");

