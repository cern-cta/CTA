/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendVFS.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

int main(int argc, char** argv) {
  std::unique_ptr<cta::objectstore::Backend> backend;

  try {
    if (argc != 2) {
      throw std::runtime_error("Wrong number of arguments: expected objectstoreURL");
    }

    std::cout << "WARNING: This is a destructive operation that will wipe the entire objectstore at " << argv[1]
              << std::endl
              << R"(Type "yes" to confirm: )";

    std::string confirmation;
    if (!std::getline(std::cin, confirmation) || confirmation != "yes") {
      std::cout << "Aborting objectstore reset" << std::endl;
      return EXIT_FAILURE;
    }

    cta::log::StdoutLogger logger(cta::utils::getShortHostname(), "cta-objectstore-reset");
    backend = cta::objectstore::BackendFactory::createBackend(argv[1], logger);

    // A VFS backend created from an explicit path is persistent by default. Keep
    // this explicit in case that implementation detail changes.
    try {
      dynamic_cast<cta::objectstore::BackendVFS&>(*backend).noDeleteOnExit();
    } catch (std::bad_cast&) {}

    const auto objects = backend->list();
    for (const auto& object : objects) {
      backend->remove(object);
    }

    std::cout << "Removed " << objects.size() << " objects from " << argv[1] << std::endl;
    return EXIT_SUCCESS;
  } catch (const std::exception& ex) {
    std::cerr << "Failed to reset " << ((backend != nullptr) ? backend->typeName() : "no-backend") << " objectstore"
              << std::endl
              << ex.what() << std::endl;
    return EXIT_FAILURE;
  }
}
