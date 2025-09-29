/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
