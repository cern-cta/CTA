/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/**
 * This program will create a VFS backend for the object store and populate
 * it with the minimum elements (the root entry). The program will then print out
 * the path the backend store and exit
 */

#include "common/config/Config.hpp"
#include "BackendFactory.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "BackendVFS.hpp"
#include "GenericObject.hpp"
#include <iostream>
#include <stdexcept>
#include <optional>
#include <common/exception/NoSuchObject.hpp>
#include "common/json/object/JSONCObject.hpp"
#include <getopt.h>

void print_help(const std::string & cmd) {
  std::cout << "Usage:" << cmd << " [--json] [--bodydump] [objectstoreURL] objectname" << std::endl;
}

int main(int argc, char ** argv) {

  static struct option longopts[] = {
    { "help",  no_argument, nullptr, 'h' },
    { "json",  no_argument, nullptr, 'j' },
    { "bodydump",  no_argument, nullptr, 'b' },
    { nullptr,           0, nullptr, 0 }
  };

  bool full_json = false;
  bool dump_object_body_only = false;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;

  for(int opt = 0; (opt = getopt_long(argc, argv, "", longopts, nullptr)) != -1; ) {
    switch(opt) {
      case 'h':
        print_help(argv[0]);
        return 0;
      break;
      case 'j':
        full_json = true;
      break;
      case 'b':
        dump_object_body_only = true;
      break;
      case '?':
      default:
      {
        print_help(argv[0]);
        throw std::runtime_error("Unknown command-line option");
      }
    }
  }

  const int nbArgs = argc - optind;

  if(nbArgs != 1 && nbArgs != 2) {
    print_help(argv[0]);
    throw std::runtime_error("Wrong number of positional arguments: expected 1 or 2: [objectstoreURL] objectname");
  }

  try {
    cta::log::DummyLogger dl("", "");
    std::unique_ptr<cta::objectstore::Backend> be;
    std::string objectName;
    if (2 == nbArgs) {
      be.reset(cta::objectstore::BackendFactory::createBackend(argv[optind], dl).release());
      objectName = argv[optind + 1];
    } else {
      cta::common::Config m_ctaConf("/etc/cta/cta-objectstore-tools.conf");
      be = std::move(
        cta::objectstore::BackendFactory::createBackend(m_ctaConf.getOptionValueStr("BackendPath").value(), dl));
      objectName = argv[optind];
    }
    // If the backend is a VFS, make sure we don't delete it on exit.
    // If not, nevermind.
    try {
      dynamic_cast<cta::objectstore::BackendVFS &>(*be).noDeleteOnExit();
    } catch (std::bad_cast &){}
    cta::objectstore::GenericObject ge(objectName, *be);
    ge.fetchNoLock();
    std::string objectStorePath = be->getParams()->toURL();
    if (dump_object_body_only) {
      cta::utils::json::object::JSONCObject jObject;
      jObject.buildFromJSON(ge.dump().second);
      std::cout << jObject.getJSONPretty() << std::endl;
    } else {
      auto [headerDump, bodyDump] = ge.dump();
      if (full_json) {
        std::ostringstream oss;
        oss << R"({)";
        oss << R"("objectStorePath":")" << objectStorePath << R"(",)";
        oss << R"("objectName":")" << objectName << R"(",)";
        oss << R"("headerDump":)" << headerDump << R"(,)";
        oss << R"("bodyDump":)" << bodyDump << R"(})";
        cta::utils::json::object::JSONCObject jObject;
        jObject.buildFromJSON(oss.str());
        std::cout << jObject.getJSONPretty() << std::endl;
      } else {
        std::cout << "Object store path: " << be->getParams()->toURL() << std::endl;
        std::cout << "Object name: " << objectName << std::endl;
        std::cout << "Header dump:" << std::endl;
        std::cout << headerDump;
        std::cout << "Body dump:" << std::endl;
        std::cout << bodyDump;
        std::cout << std::endl;
      }
    }
  } catch (cta::exception::NoSuchObject &) {
    std::cerr << "Object not found in the object store" << std::endl;
  } catch (const std::bad_optional_access&) {
    std::cerr << "Config file '/etc/cta/cta-objectstore-tools.conf' does not contain the BackendPath entry." << std::endl;
  } catch (std::exception& e) {
    std::cerr << "Failed to dump object: "
        << std::endl << e.what() << std::endl;
  }
}
