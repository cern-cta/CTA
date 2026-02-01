/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdApp.hpp"
#include "MaintdConfig.hpp"
#include "common/runtime/Application.hpp"
#include "common/runtime/ArgParser.hpp"
#include "common/runtime/CommonCliOptions.hpp"

int main(const int argc, char** const argv) {
  using namespace cta;
  try {
    const std::string appName = "cta-maind";
    runtime::ArgParser<CommonCliOptions> argParser(appName);
    auto cliOptions = argParser.parse(argc, argv);
    runtime::Application<maintd::MaintdApp, maintd::MaintdConfig> app(appName, cliOptions);
    return app.run();
  } catch (...) {
    // TODO: we probably want to do some printing here
    return EXIT_FAILURE;
  }
}
