#include "threading/Threading.hpp"
#include "threading/ChildProcess.hpp"
#include "exception/Exception.hpp"
#include "exception/Errnum.hpp"
#include <iostream>
#include "ObjectStoreChoice.hpp"
#include "RootEntry.hpp"
#include "Action.hpp"
#include "ContextHandle.hpp"
#include "ObjectStructureDumper.hpp"
#include "JobPool.hpp"



class jobExecutorThread: public cta::threading::Thread {
public:
  jobExecutorThread(Action & a): cta::threading::Thread(), m_action(a) {}
private:
  virtual void run () {
    // make the agent act
    m_action.execute();
  }
  Action & m_action;
};

class jobExecutorProcess: public cta::threading::ChildProcess {
public:
  jobExecutorProcess(Action & a): cta::threading::ChildProcess(), m_action(a) {}
private:
  virtual int run () {
    // increase 100 time root entry's 
    try {
      m_action.execute();
    } catch (std::exception & e) {
      std::cout << e.what() << std::endl;
    } catch (...) {
      return -1;
    }
    return 0;
  }
  Action & m_action;
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
    
    // Create our own agent representation
    Agent self(os, "masterProcess");
    self.create();
    ContextHandleImplementation<myOS> ctx;
    // Dump the structure
    ObjectStrucutreDumper osd;
    std::cout << osd.dump(self) << std::endl;
    // Get hold of the root entry
    RootEntry re(self);
    // Create and populate the job queues
    Register agentRegister(re.allocateOrGetAgentRegister(self), self);
    JobPool jobPool(re.allocateOrGetJobPool(self), self);
    // Dump again
    std::cout << osd.dump(self) << std::endl;
  } catch (std::exception &e) {
    std::cout << "got exception: " << e.what() << std::endl;
  }
  
  return 0;
}
