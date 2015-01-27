#include "threading/Threading.hpp"
#include "threading/ChildProcess.hpp"
#include "exception/Exception.hpp"
#include "exception/Errnum.hpp"
#include <iostream>
#include "ObjectStores.hpp"
typedef ObjectStoreRados myOS;
#include "RootEntry.hpp"
#include "Agent.hpp"
#include "ContextHandle.hpp"
#include "AgentId.hpp"
#include "ObjectStructureDumper.hpp"
#include "JobPool.hpp"



class jobExecutorThread: public cta::threading::Thread {
public:
  jobExecutorThread(Agent & a): cta::threading::Thread(), m_agent(a) {}
private:
  virtual void run () {
    // make the agent act
    m_agent.act();
  }
  Agent & m_agent;
};

class jobExecutorProcess: public cta::threading::ChildProcess {
public:
  jobExecutorProcess(Agent & a): cta::threading::ChildProcess(), m_agent(a) {}
private:
  virtual int run () {
    // increase 100 time root entry's 
    try {
      m_agent.act();
    } catch (std::exception & e) {
      std::cout << e.what() << std::endl;
    } catch (...) {
      return -1;
    }
    return 0;
  }
  Agent & m_agent;
};

class dummyCleanup: public cta::threading::ChildProcess::Cleanup {
  virtual void operator()() {}
} dc;

int main(void){
  try {
    myOS os("", "tapetest", "tapetest");
    //myOS os;
    std::cout << "os.path=" << os.path() << " os.user=" << os.user() 
      << "os.pool=" << os.pool() << std::endl;
    // Initialize the root entry
    RootEntry::init(os);
    ContextHandleImplementation<myOS> ctx;
    // Dump the structure
    ObjectStrucutreDumper osd;
    std::cout << osd.dump(os, ctx) << std::endl;
    // Get hold of the root entry
    RootEntry re(os,ctx);
    // Create and populate the job queues
    AgentId aid("masterProcess");
    Register agentRegister(os, re.allocateOrGetJobPool(ctx, aid.nextId()), ctx);
    JobPool jobPool(os, re.allocateOrGetJobPool(ctx, aid.nextId()), ctx);
    // Dump again
    std::cout << osd.dump(os, ctx) << std::endl;
  } catch (std::exception &e) {
    std::cout << "got exception: " << e.what() << std::endl;
  }
  
  return 0;
}
