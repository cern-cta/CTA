#include "ObjectStoreChoice.hpp"
#include "Action.hpp"
#include "exception/Exception.hpp"
#include <iostream>
#include <stdint.h>

int main(int argc, char** argv) {
  try {
#if USE_RADOS
    myOS os("", "tapetest", "tapetest");
#else
    myOS os;
    os.noDeleteOnExit();
#endif
    std::cout << "os.path=" << os.path() << " os.user=" << os.user() 
      << "os.pool=" << os.pool() << std::endl;
    cta::objectstore::RootEntry::init(os);
    cta::objectstore::Agent agent(os, "CommandLineCreateEnvironment");
    agent.create();
    // Get hold of the root entry
    cta::objectstore::RootEntry re(agent);
    // Create the job pool
    std::cout << "=============== About to add job pool" << std::endl;
    cta::objectstore::JobPool jobPool(re.allocateOrGetJobPool(agent), agent);
    
    // Create the recall FIFO
    std::cout << "=============== About to add recall FIFO" << std::endl;
    cta::objectstore::FIFO recallFIFO(jobPool.allocateOrGetRecallFIFO(agent), agent);
    // Create the counter
    std::cout << "=============== About to add recall counter" << std::endl;
    cta::objectstore::Counter recallCounter(jobPool.allocateOrGetRecallCounter(agent), agent);
  } catch (std::exception & e) {
    std::cout << "Got exception: " << e.what();
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
