/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "common/runtime/Application.hpp"
#include "common/runtime/ArgParser.hpp"
#include "common/runtime/CommonCliOptions.hpp"

int main(const int argc, char** const argv) {
  // TODO: sadly this will have to be wrapped in a try catch
  using namespace cta;
  const std::string appName = "cta-maind";

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  argParser.parser(argc, argv);
  // At this point, opts is populated

  // TODO: can opts be inferred?
  runtime::Application<maintd::RoutineRunner, maintd::MaintdConfig, runtime::CommonCliOptions> app(appName, opts);
  // Starts the RoutineRunner
  return app.run();
}
