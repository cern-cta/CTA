#include "ObjectStoreChoice.hpp"
#include "Action.hpp"
#include "exception/Exception.hpp"
#include <iostream>
#include <stdint.h>

int main(int argc, char** argv) {
  if (argc != 5) {
    std::cout << "Expected 4 parameters: <path for VFS storage> <username for Ceph> <pool for Ceph>" << std::endl;
    return EXIT_FAILURE;
  }
  try {
    myOS os(argv[1], argv[2], argv[3]);
    uint64_t count;
    if (! (std::istringstream(argv[4]) >> count)) {
      throw cta::exception::Exception("Cound not convert 4th argument into a number");
    }
    cta::objectstore::Agent agent(os);
    cta::objectstore::JobPoster poster(agent, 0, count);
    poster.execute();
  } catch (std::exception & e) {
    std::cout << "Got exception: " << e.what();
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
