/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "WorkflowApiApp.hpp"
#include "WorkflowApiConfig.hpp"
#include "runtime/Application.hpp"

const std::string APP_NAME = "cta-workflow-api;";
const std::string DESCRIPTION = R"""(
Workflow API - a gRPC service receiving the archive/retrieve workflow events of the CTA frontend
  )""";

int main(const int argc, char** const argv) {
  using namespace cta;
  return runtime::safeRun([argc, argv]() {
    using App =
      runtime::Application<workflow_api::WorkflowApiApp, workflow_api::WorkflowApiConfig, runtime::CommonCliOptions>;
    App app(APP_NAME, DESCRIPTION);
    return app.run(argc, argv);
  });
}
