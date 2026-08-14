/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RmcdApp.hpp"
#include "RmcdConfig.hpp"
#include "common/runtime/Application.hpp"

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    const std::string appName = "cta-rmcd";
    const std::string description = R"""(

  )""";
    using App = runtime::Application<rmcd::RmcdApp, rmcd::RmcdConfig, runtime::CommonCliOptions>;
    App app(appName, description);
    return app.run(argc, argv);
  });
}
