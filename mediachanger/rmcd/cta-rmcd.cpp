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
Daemon responsible for controlling a SCSI-compatible robotic tape library on behalf of
RMC clients. It handles library queries and cartridge mount, dismount, import and export
requests received over the RMC protocol.
  )""";
    using App = runtime::Application<rmcd::RmcdApp, rmcd::RmcdConfig, runtime::CommonCliOptions>;
    App app(appName, description);
    return app.run(argc, argv);
  });
}
