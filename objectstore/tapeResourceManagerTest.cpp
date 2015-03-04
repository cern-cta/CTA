#include "threading/Threading.hpp"
#include "threading/ChildProcess.hpp"
#include "exception/Exception.hpp"
#include "exception/Errnum.hpp"
#include <iostream>
#include "ObjectStoreChoice.hpp"
#include "RootEntry.hpp"
#include "Action.hpp"
#include "ObjectStructureDumper.hpp"
#include "JobPool.hpp"
#include "AgentRegister.hpp"
#include "RecallJob.hpp"
#include "Register.hpp"
#include "AgentVisitor.hpp"
#include "Counter.hpp"
#include <math.h>

namespace systemTests { 

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

#define USE_PROCESS 0
#if(USE_PROCESS)
  class dummyCleanup: public cta::threading::ChildProcess::Cleanup {
    virtual void operator()() {}
  } dc;
  typedef jobExecutorProcess jobExecutor;
#else
  #define dc
  typedef jobExecutorThread jobExecutor;
#endif

int main(void){
  try {
#if USE_RADOS
    myOS os("", "tapetest", "tapetest");
#else
    myOS os;
#endif
    std::cout << "os.path=" << os.path() << " os.user=" << os.user() 
      << "os.pool=" << os.pool() << std::endl;
    // Initialize the root entry
    cta::objectstore::RootEntry::init(os);
    
    // Create our own agent representation
    cta::objectstore::Agent self(os, "masterProcess");
    self.create();
    cta::objectstore::ContextHandle ctx;
    // Dump the structure
    cta::objectstore::ObjectStructureDumper osd;
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
    // Create the counter
    std::cout << "=============== About to add recall counter" << std::endl;
    cta::objectstore::Counter recallCounter(jobPool.allocateOrGetRecallCounter(self), self);
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
    cta::objectstore::Agent gcAgent(gcObjectStore, "injectorProcess");
    cta::objectstore::GarbageCollector gcAction(gcAgent);
    jobExecutor gcProcess(gcAction);
    gcProcess.start(dc);
    
    std::vector<cta::objectstore::Backend *> recallObjectStores;
    std::vector<cta::objectstore::Agent *> recallAgents;
    std::vector<cta::objectstore::Action *> recallActions;
    std::vector<jobExecutor *> recallJobs;
  
    // Loop start-kill the recallers
    // get hold of the recall FIFO
    // create 10 recallers
    int recallerNumber = 0;
    for (int i=0; i< 10 ; i++) {
      // Prepare the object stores and agents for the recaller
      recallObjectStores.push_back(new myOS(os.path(), os.user(), os.pool()));
      recallAgents.push_back(new cta::objectstore::Agent(*recallObjectStores[i]));
      recallActions.push_back(new cta::objectstore::Recaller(*recallAgents[i], recallerNumber++));
      recallJobs.push_back(new jobExecutor(*recallActions[i]));
      recallJobs.back()->start(dc);
    }
    std::cout << "Initial jobs creation done."
      << " recallJobs.size()=" << recallJobs.size()
      << " recallActions.size()=" << recallActions.size()
      << " recallAgents.size()=" << recallAgents.size()
      << " recallObjectStores.size()=" << recallObjectStores.size() << std::endl;
    usleep (100*1000);
    struct random_data seed;
    memset(&seed, 0, sizeof(seed));
    char seedStr[]="1234567890abcdef";
    initstate_r(1,seedStr, sizeof(seedStr), &seed);
    while(recallFIFO.size(self)) {
      std::cout << "Kill-restart recaller, checkpoint 1" << std::endl;
      // Kill a recaller, start another one
      int32_t ranInt;
      random_r(&seed, &ranInt);
      size_t i = floor(1.0 * ranInt / RAND_MAX * recallJobs.size());
      std::cout << "Kill-restart recaller, checkpoint 2" << std::endl;
      if (i >= recallJobs.size()) i= recallJobs.size() - 1;
      // Kill the job
      std::cout << "Kill-restart recaller, checkpoint 3 i=" << i << std::endl;
      recallJobs[i]->kill();
      std::cout << "Kill-restart recaller, checkpoint 4" << std::endl;
      try {
        recallJobs[i]->wait();
      } catch (cta::exception::Exception & e) {
        std::cout << "Got exception while waiting (after killing) for recallJobs[" << i << "]:"
            << std::endl
            << e.what();
      }
      std::cout << "Kill-restart recaller, checkpoint 5" << std::endl;
      delete recallJobs[i];
      std::cout << "Kill-restart recaller, checkpoint 6" << std::endl;
      recallJobs.erase(recallJobs.begin() + i);
      delete recallActions[i];
      recallActions.erase(recallActions.begin() + i);
      delete recallAgents[i];
      recallAgents.erase(recallAgents.begin() + i);
      delete recallObjectStores[i];
      recallObjectStores.erase(recallObjectStores.begin() + i);
      // start a new one
      i = recallJobs.size();
      // Prepare the object stores and agents for the recaller
      recallObjectStores.push_back(new myOS(os.path(), os.user(), os.pool()));
      recallAgents.push_back(new cta::objectstore::Agent(*recallObjectStores[i]));
      recallActions.push_back(new cta::objectstore::Recaller(*recallAgents[i], recallerNumber++));
      recallJobs.push_back(new jobExecutor(*recallActions[i]));
      recallJobs.back()->start(dc);
    }
    // wait for recalls to be done
    while (recallCounter.get(self) < 100) {
      usleep (100 * 1000);
    }

    while (recallJobs.size()) {
      // Wait for completion the job
      try {
        recallJobs.back()->wait();
      } catch (cta::exception::Exception & e) {
        std::cout << "Got exception while waiting for recallJobs[" << recallJobs.size() -1 << "]:"
            << std::endl
            << e.what();
      }
      std::cout << "recallJobs[" << recallJobs.size()-1 << "] completed. Starting cleanup with"
        << " recallJobs.size()=" << recallJobs.size()
        << " recallActions.size()=" << recallActions.size()
        << " recallAgents.size()=" << recallAgents.size()
        << " recallObjectStores.size()=" << recallObjectStores.size() << std::endl;
      delete recallJobs.back();
      recallJobs.pop_back();
      delete recallActions.back();
      recallActions.pop_back();
      delete recallAgents.back();
      recallAgents.pop_back();
      delete recallObjectStores.back();
      recallObjectStores.pop_back();
      std::cout << "Cleanup done with"
        << " recallJobs.size()=" << recallJobs.size()
        << " recallActions.size()=" << recallActions.size()
        << " recallAgents.size()=" << recallAgents.size()
        << " recallObjectStores.size()=" << recallObjectStores.size() << std::endl;
    }
    
    try {
      injectorProcess.wait();
    } catch (cta::exception::Exception & e) {
      std::cout << "Got exception while waiting for injectorProcess:"
          << std::endl
          << e.what();
    }
    gcProcess.kill();
    try {
      gcProcess.wait();
    } catch (cta::exception::Exception & e) {
      std::cout << "Got exception while waiting for gcProcess:"
          << std::endl
          << e.what();
    }
    gcAgent.flushContexts();
    
    // And see the state or affairs
    std::cout << osd.dump(self) << std::endl;
  } catch (std::exception &e) {
    std::cout << "At top level of resource manager test, got exception: " << e.what() << std::endl;
  }
  
  // Create the job poster
  
  
  return 0;
}

}
