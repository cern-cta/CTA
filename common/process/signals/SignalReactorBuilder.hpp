/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SignalReactor.hpp"
#include "common/log/LogContext.hpp"

#include <functional>
#include <unordered_map>

namespace cta::process {

/**
 * This builder allows for the construction of an immutable SignalReactor object
 */
class SignalReactorBuilder {
public:
  explicit SignalReactorBuilder(cta::log::LogContext& lc);

  SignalReactorBuilder& addSignalFunction(int signal, const std::function<void()>& func);

  SignalReactor build();

private:
  cta::log::LogContext& m_lc;
  std::unordered_map<int, std::function<void()>> m_signalFunctions;
  sigset_t m_sigset;
};

}  // namespace cta::process
