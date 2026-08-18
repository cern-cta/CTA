/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/**
 * This program will create a VFS backend for the object store and populate
 * it with the minimum elements (the root entry). The program will then print out
 * the path the backend store and exit
 */

#include "Agent.hpp"
#include "BackendFactory.hpp"
#include "BackendVFS.hpp"
#include "RootEntry.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

#include <iostream>
#include <stdexcept>

int main(int argc, char** argv) {
  try {
    cta::log::DummyLogger dl("", "");
    std::unique_ptr<cta::objectstore::Backend> be;
    if (1 == argc) {
      be = std::move(cta::objectstore::BackendFactory::createBackend(
        cta::utils::readSingleLineConfigFile("/etc/cta/cta-scheduler.conf"),
        dl));
    } else if (2 == argc) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[1], dl).release());
    } else {
      throw std::runtime_error("Wrong number of arguments: expected 0 or 1: [objectstoreURL]");
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    try {
      dynamic_cast<cta::objectstore::BackendVFS&>(*be).noDeleteOnExit();
    } catch (std::bad_cast&) {}
    // If not, nevermind.
    auto params = be->getParams();
    std::cout << "Object store path: " << params->toURL() << std::endl;
    auto l = be->list();
    for (auto o = l.begin(); o != l.end(); o++) {
      std::cout << *o << std::endl;
    }
  } catch (std::exception& e) {
    std::cerr << "Failed to list backend store: " << std::endl << e.what() << std::endl;
  }
}
