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
#include "AgentRegister.hpp"
#include "RecallJob.hpp"
#include "Register.hpp"
#include "AgentVisitor.hpp"



class jobExecutorThread: public cta::threading::Thread {
public:
  jobExecutorThread(cta::objectstore::Action & a): 
    cta::threading::Thread(), m_action(a) {}
private:
  virtual void run () {
    // execute the action
    m_action.execute();
  }
  cta::objectstore::Action & m_action;
};

class jobExecutorProcess: public cta::threading::ChildProcess {
public:
  jobExecutorProcess(cta::objectstore::Action & a):
    cta::threading::ChildProcess(), m_action(a) {}
private:
  virtual int run () {
    // execute the action 
    try {
      m_action.execute();
    } catch (std::exception & e) {
      std::cout << e.what() << std::endl;
    } catch (...) {
      return -1;
    }
    return 0;
  }
  cta::objectstore::Action & m_action;
};

class dummyCleanup: public cta::threading::ChildProcess::Cleanup {
  virtual void operator()() {}
} dc;

typedef jobExecutorProcess jobExecutor;

int main(void){
  std::vector<cta::objectstore::ObjectStore *> recallObjectStores;
  std::vector<cta::objectstore::Agent *> recallAgents;
  std::vector<cta::objectstore::Action *> recallActions;
  std::vector<jobExecutor *> recallJobs;
  try {
    myOS os("", "tapetest", "tapetest");
    //myOS os;
    std::cout << "os.path=" << os.path() << " os.user=" << os.user() 
      << "os.pool=" << os.pool() << std::endl;
    // Initialize the root entry
    cta::objectstore::RootEntry::init(os);
    
    // Create our own agent representation
    cta::objectstore::Agent self(os, "masterProcess");
    self.create();
    cta::objectstore::ContextHandleImplementation<myOS> ctx;
    // Dump the structure
    cta::objectstore::ObjectStrucutreDumper osd;
    std::cout << osd.dump(self) << std::endl;
    // Get hold of the root entry
    cta::objectstore::RootEntry re(self);
    // Create and populate the job queues
    std::cout << "About to add agentRegister" << std::endl;
    cta::objectstore::AgentRegister agentRegister(re.allocateOrGetAgentRegister(self), self);
    std::cout << osd.dump(self) << std::endl;
    
    // Create the job pool
    std::cout << "=============== About to add job pool" << std::endl;
    cta::objectstore::JobPool jobPool(re.allocateOrGetJobPool(self), self);
    
    // Create the recall FIFO
    std::cout << "=============== About to add recall FIFO" << std::endl;
    cta::objectstore::FIFO recallFIFO(jobPool.allocateOrGetRecallFIFO(self), self);
    // Dump again
    std::cout << osd.dump(self) << std::endl;
    
    // start the job injector
    myOS injectorObjectStore(os.path(), os.user(), os.pool());
    cta::objectstore::Agent injectorAgent(injectorObjectStore, "injectorProcess");
    cta::objectstore::JobPoster injectorAction(injectorAgent, 0, 100);
    jobExecutor injectorProcess(injectorAction);
    injectorProcess.start(dc);
    
    // start the garbage collector
    myOS gcObjectStore(os.path(), os.user(), os.pool());
    cta::objectstore::Agent gcAgent(injectorObjectStore, "injectorProcess");
    cta::objectstore::GarbageCollector gcAction(injectorAgent);
    jobExecutor gcProcess(injectorAction);
    injectorProcess.start(dc);
    
    // Look start-kill the recallers
    // get hold of the recall FIFO
    // create 10 recallers
    //int recallerNumber = 0;
    for (int i=0; i< 10 ; i++) {
      
    }
    usleep (100*1000);
//    while(recallFIFO.size(self)) {
//      
//    }
    
  } catch (std::exception &e) {
    std::cout << "got exception: " << e.what() << std::endl;
  }
  
  // Create the job poster
  
  
  return 0;
}
