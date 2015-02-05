#include "RootEntry.hpp"

#include "Agent.hpp"
#include <cxxabi.h>

// Initializer.
void cta::objectstore::RootEntry::init(ObjectStore & os) {
  // check existence of root entry before creating it. We expect read to fail.
  try {
    os.read(s_rootEntryName);
    throw cta::exception::Exception("In RootEntry::init: root entry already exists");
  } catch (std::exception&) {
  }
  serializers::RootEntry res;
  os.create(s_rootEntryName, res.SerializeAsString());
}
// construtor, when the backend store exists.
// Checks the existence and correctness of the root entry
cta::objectstore::RootEntry::RootEntry(Agent & agent):
  ObjectOps<serializers::RootEntry>(agent.objectStore(), s_rootEntryName) {
  // Check that the root entry is readable.
  serializers::RootEntry res;
  updateFromObjectStore(res, agent.getFreeContext());
}

// Get the name of the agent register (or exception if not available)
std::string cta::objectstore::RootEntry::getAgentRegister(Agent & agent) {
  // Check if the agent register exists
  serializers::RootEntry res;
  updateFromObjectStore(res, agent.getFreeContext());
  // If the registry is defined, return it, job done.
  if (res.agentregister().size())
    return res.agentregister();
  throw NotAllocatedEx("In RootEntry::getAgentRegister: agentRegister not yet allocated");
}

// Get the name of a (possibly freshly created) agent register
std::string cta::objectstore::RootEntry::allocateOrGetAgentRegister(Agent & agent) {
  // Check if the agent register exists
  try {
    return getAgentRegister(agent);
  } catch (NotAllocatedEx &) {
    // If we get here, the agent register is not created yet, so we have to do it:
    // lock the entry again, for writing
    serializers::RootEntry res;
    ContextHandle & context = agent.getFreeContext();
    lockExclusiveAndRead(res, context, "RootEntry::allocateOrGetAgentRegister");
    // If the registry is already defined, somebody was faster. We're done.
    if (res.agentregister().size()) {
      unlock(context);
      return res.agentregister();
    }
    // We will really create the register
    // decide on the object's name
    std::string arName (agent.nextId("agentRegister"));
    // Record the agent in the intent log
    res.add_agentregisterintentlog(arName);
    // Commit the intents
    write(res);
    // The potential object can now be garbage collected if we die from here.
    // Create the object, then lock. The name should be unique, so no race.
    serializers::Register ars;
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
std::string cta::objectstore::RootEntry::getJobPool(Agent & agent) {
  // Check if the job pool exists
  serializers::RootEntry res;
  updateFromObjectStore(res, agent.getFreeContext());
  // If the registry is defined, return it, job done.
  if (res.jobpool().size())
    return res.jobpool();
  throw NotAllocatedEx("In RootEntry::getJobPool: jobPool not yet allocated");
}

// Get the name of a (possibly freshly created) job pool
std::string cta::objectstore::RootEntry::allocateOrGetJobPool(Agent & agent) {
  // Check if the job pool exists
  try {
    return getJobPool(agent);
  } catch (NotAllocatedEx &) {
    // If we get here, the job pool is not created yet, so we have to do it:
    // lock the entry again, for writing
    serializers::RootEntry res;
    ContextHandle & context = agent.getFreeContext();
    lockExclusiveAndRead(res, context, "RootEntry::allocateOrGetJobPool");
    // If the registry is already defined, somebody was faster. We're done.
    if (res.jobpool().size()) {
      unlock(context);
      return res.jobpool();
    }
    // We will really create the register
    // decide on the object's name
    std::string jpName (agent.nextId("jobPool"));
    // Record the agent in the intent log
    agent.addToIntend(s_rootEntryName, jpName, serializers::JobPool_t);
    // The potential object can now be garbage collected if we die from here.
    // Create the object, then lock. The name should be unique, so no race.
    serializers::JobPool jps;
    jps.set_migration("");
    jps.set_recall("");
    writeChild(jpName, jps);
    // If we lived that far, we can update the root entry to point to our
    // new agent register, and remove the name from the intent log.
    res.set_jobpool(jpName);
    write(res);
    // release the lock, and return the register name
    unlock(context);
    // Clear intent log
    agent.removeFromIntent(s_rootEntryName, jpName, serializers::JobPool_t);
    return jpName;
  }
}

// Dump the root entry
std::string cta::objectstore::RootEntry::dump (Agent & agent) {
  std::stringstream ret;
  serializers::RootEntry res;
  updateFromObjectStore(res, agent.getFreeContext());
  ret << "<<<< Root entry dump start" << std::endl;
  if (res.has_agentregister()) ret << "agentRegister=" << res.agentregister() << std::endl;
  ret << "agentRegister Intent Log size=" << res.agentregisterintentlog_size() << std::endl;
  for (int i=0; i<res.agentregisterintentlog_size(); i++) {
    ret << "agentRegisterIntentLog=" << res.agentregisterintentlog(i) << std::endl;
  }
  if (res.has_jobpool()) ret << "jobPool=" << res.jobpool() << std::endl;
  if (res.has_driveregister()) ret << "driveRegister=" << res.driveregister() << std::endl;
  if (res.has_taperegister()) ret << "tapeRegister=" << res.taperegister() << std::endl;
  ret << ">>>> Root entry dump start" << std::endl;
  return ret.str();
}

const std::string cta::objectstore::RootEntry::s_rootEntryName("root");