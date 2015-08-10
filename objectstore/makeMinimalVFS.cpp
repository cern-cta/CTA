/*
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

/**
 * This program will create a VFS backend for the object store and populate
 * it with the minimum elements (the root entry). The program will then print out
 * the path the backend store and exit
 */

#include "BackendVFS.hpp"
#include "RootEntry.hpp"
#include <iostream>

int main(void) {
  try {
    cta::objectstore::BackendVFS be;
    be.noDeleteOnExit();
    cta::objectstore::RootEntry re(be);
    re.initialize();
    re.insert();
    std::cout << "New object store path: " << be.getParams()->getPath() << std::endl;
  } catch (std::exception & e) {
    std::cerr << "Failed to initialise the root entry in a new VFS backend store"
        << std::endl << e.what() << std::endl;
  }
}