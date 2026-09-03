/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <string>

namespace cta::auth {
CTA_GENERATE_EXCEPTION_CLASS(CurlException);

class JwksFetcher {
public:
  virtual ~JwksFetcher() = default;
  virtual std::string fetchJWKS(const std::string& jwksUrl) = 0;
};

class CurlJwksFetcher : public JwksFetcher {
public:
  explicit CurlJwksFetcher(int totalTimeoutSecs);
  ~CurlJwksFetcher() override;

  // Delete copy/move to ensure single instance manages curl global state
  CurlJwksFetcher(const CurlJwksFetcher&) = delete;
  CurlJwksFetcher& operator=(const CurlJwksFetcher&) = delete;
  CurlJwksFetcher(CurlJwksFetcher&&) = delete;
  CurlJwksFetcher& operator=(CurlJwksFetcher&&) = delete;

  std::string fetchJWKS(const std::string& jwksUrl) override;

private:
  long m_totalTimeoutSecs;  //!< Total timeout in seconds
};
}  // namespace cta::auth
