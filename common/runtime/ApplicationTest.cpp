/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Application.hpp"

#include "CommonCliOptions.hpp"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <thread>

namespace unitTests {

TEST(Application, SimpleApp) {
  const std::string appName = "cta-test";
  CommonCliOptions opts;

  runtime::Application<maintd::RoutineRunner, maintd::MaintdConfig> app(appName, opts);
  // Starts the RoutineRunner
  return app.run();
}

TEST(Application, AppNameCannotBeEmpty) {}

TEST(Application, SimpleAppWithCustomCliOpts) {}

TEST(Application, AppThatDoesNotConsumeCliOpts) {}

TEST(Application, AppWithCustomConfig) {}

TEST(Application, AppWithHealthServer) {}

TEST(Application, AppHandlesSigTerm) {}

TEST(Application, AppCorrectXRootDKeyTab) {}

TEST(Application, AppWrongXRootDKeyTab) {}

}  // namespace unitTests
