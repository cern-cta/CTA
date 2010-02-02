// Include python.hpp before any standard headers because python.hpp includes
// Python.h which may define some pre-processor definitions which affect the
// standard headers
#include "castor/tape/python/python.hpp"

#include <iostream>
#include <pthread.h>




int main(int argc, char **argv) {

  std::cout <<
    "\n"
    "Testing castor::tape::python\n"
    "============================\n" <<
    std::endl;

  return 0;
}
