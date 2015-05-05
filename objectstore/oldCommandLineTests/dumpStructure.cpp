#include "ObjectStructureDumper.hpp"
#include "ObjectStoreChoice.hpp"
#include <exception>
#include <iostream>

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cout << "Expected 3 parameters: <path for VFS storage> <username for Ceph> <pool for Ceph>" << std::endl;
    return EXIT_FAILURE;
  }
  try {
    myOS os(argv[1], argv[2], argv[3]);
    cta::objectstore::Agent agent(os, "CommandLineDumper");
    agent.create();
    cta::objectstore::ObjectStructureDumper osd;
    std::cout << osd.dump(agent);
  } catch (std::exception & e) {
    std::cout << "Got exception: " << e.what();
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}