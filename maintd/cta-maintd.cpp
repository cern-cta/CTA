/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MaintdCliOptions.hpp"
#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "RoutineRunnerFactory.hpp"
#include "common/runtime/Application.hpp"

#include <getopt.h>
#include <iostream>
#include <memory>
#include <signal.h>
#include <string>
#include <thread>

int main(const int argc, char** const argv) {
  using namespace cta;
  maintd::MaintdCliOptions opts;
  runtime::Application<maintd::RoutineRunner, maintd::MaintdConfig, maintd::MaintdCliOptions> app("cta-maintd", opts);
  return app.run();
}
