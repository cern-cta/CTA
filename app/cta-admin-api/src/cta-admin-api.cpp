/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AdminApiApp.hpp"
#include "AdminApiConfig.hpp"
#include "runtime/Application.hpp"

const std::string APP_NAME = "cta-admin-api;";
const std::string DESCRIPTION = R"""(
Admin API - a gRPC service providing the admin commands of the CTA frontend
)""";

/**
 * main() function, which calls the actual application code,
 * handled by the runtime
 */
int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    using App = runtime::Application<admin_api::AdminApiApp, admin_api::AdminApiConfig, runtime::CommonCliOptions>;
    App app(APP_NAME, DESCRIPTION);
    return app.run(argc, argv);
  });
}
