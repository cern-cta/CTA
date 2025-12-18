/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/Timer.hpp"

#include <optional>
#include <string>

namespace cta::frontend {

// Used to track ctaFrontendActiveRequests
class RequestTracker {
public:
  RequestTracker(std::string_view eventName, std::string_view requesterName);
  ~RequestTracker();
  RequestTracker(const RequestTracker&) = delete;
  RequestTracker& operator=(const RequestTracker&) = delete;

  void setErrorType(std::string_view errorType);

private:
  utils::Timer m_timer;
  std::string m_eventName;
  std::string m_requesterName;
  std::optional<std::string> m_errorType;
};

}  // namespace cta::frontend
