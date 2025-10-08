/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <opentelemetry/sdk/common/global_log_handler.h>
#include "common/log/Logger.hpp"

namespace cta::telemetry {

/**
 * An implementation of the internal log handler from OpenTelemetry that ensures the internal
 * OpenTelemetry logs are written in the same file and format as the CTA logs.
 * Note that by default, this only concerns errors and should therefore not pollute the logs unnecessarily.
 */
class CtaTelemetryLogHandler : public opentelemetry::sdk::common::internal_log::LogHandler {
public:
  explicit CtaTelemetryLogHandler(log::Logger& log);
  ~CtaTelemetryLogHandler() override;

  void Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
              const char* file,
              int line,
              const char* msg,
              const opentelemetry::sdk::common::AttributeMap& attributes) noexcept override;

private:
  log::Logger& m_log;
};

}  // namespace cta::telemetry
