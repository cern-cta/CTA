/**
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
