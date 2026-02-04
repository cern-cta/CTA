/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdApp.hpp"
#include "MaintdConfig.hpp"
#include "common/runtime/Application.hpp"
#include "common/runtime/CommonCliOptions.hpp"

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    const std::string appName = "cta-maintd";
    const std::string description = R"""(
Daemon responsible for periodically executing a set of routines. These routines are
responsible for things such as Reporting archive/retrieve status to the disk buffer,
handling repack requests, garbage collection, and queue cleanup in the scheduler.
  )""";
    using App = runtime::Application<maintd::MaintdApp, maintd::MaintdConfig, runtime::CommonCliOptions>;
    App app(appName, description);
    return app.run(argc, argv);
  });
}
