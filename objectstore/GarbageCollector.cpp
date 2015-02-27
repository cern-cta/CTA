#include "GarbageCollector.hpp"
#include "RootEntry.hpp"

namespace cta { namespace objectstore {
  const size_t GarbageCollector::c_maxWatchedAgentsPerGC = 2;

  GarbageCollector::GarbageCollector(Backend & os, Agent & agent): 
    m_objectStore(os), m_ourAgent(agent), m_agentRegister(os) {
    RootEntry re(m_objectStore);
    ScopedSharedLock reLock(re);
    re.fetch();
    m_agentRegister.setName(re.getAgentRegister());
    reLock.release();
    ScopedSharedLock arLock(m_agentRegister);
    m_agentRegister.fetch();
  }
  
  void GarbageCollector::runOnePass() {
    // Bump our own heart beat
    {
      ScopedExclusiveLock lock (m_ourAgent);
      m_ourAgent.fetch();
      m_ourAgent.bumpHeartbeat();
      m_ourAgent.commit();
    }
    trimGoneTargets();
    aquireTargets();
    checkHeartbeats();
  }
  
  void GarbageCollector::trimGoneTargets() {
    
  }
}}
