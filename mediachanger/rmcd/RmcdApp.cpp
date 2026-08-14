/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RmcdApp.hpp"

#include "rmc_serv.hpp"

namespace cta::rmcd {

void RmcdApp::stop() {
  // ?
}

int RmcdApp::run(const RmcdConfig& config, cta::log::Logger& log) {
  cta::log::LogContext lc(log);
  return rmc_main(config.media_changer.device, config.server.port, config.server.listen_scope, lc);
}

bool RmcdApp::isReady() const {
  return true;  // For now we are always ready
}

bool RmcdApp::isLive() const {
  return true;  // For now we are always alive
}

}  // namespace cta::rmcd
