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
