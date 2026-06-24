/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TapedApp.hpp"

namespace cta::taped {

void TapedApp::stop() {}

int TapedApp::run(const TapedConfig& config, cta::log::Logger& log) {
  return 0;
}

bool TapedApp::isReady() const {
  return false;
}

bool TapedApp::isLive() const {
  return false;
}

}  // namespace cta::taped
