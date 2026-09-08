/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RmcdConfig.hpp"
#include "common/log/LogContext.hpp"

#include <stop_token>

namespace cta::rmcd {

class RmcdApp final {
public:
  RmcdApp() = default;

  ~RmcdApp() = default;

  void stop();

  int run(const RmcdConfig& config, cta::log::Logger& log);

  bool isLive() const;

  bool isReady() const;

private:
  std::stop_source m_stopSource;
};

}  // namespace cta::rmcd
