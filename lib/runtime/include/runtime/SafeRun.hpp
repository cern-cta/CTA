/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/Logger.hpp"
#include "common/semconv/Attributes.hpp"

#include <concepts>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <type_traits>
#include <unistd.h>

namespace cta::runtime {

namespace detail {

inline void reportDiagnosticFailure() noexcept {
  // Bypass streams and allocation. Ignore write errors; the caller still returns EXIT_FAILURE.
  constexpr char message[] = "FATAL: Application failed; unable to report exception details.\n";
  [[maybe_unused]] const auto written = ::write(STDERR_FILENO, message, sizeof(message) - 1);
}

}  // namespace detail

/**
 * Reports exceptions to stderr and returns EXIT_FAILURE, even if preparing or writing the diagnostic throws.
 */
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRun(F&& func) noexcept {
  try {
    try {
      return func();
    } catch (const exception::UserError& ex) {
      std::cerr << "FATAL:\n" << ex.getMessage().str() << std::endl;
    } catch (const exception::Exception& ex) {
      std::cerr << "FATAL: Caught an unexpected CTA exception:\n" << ex.getMessage().str() << std::endl;
    } catch (const std::exception& ex) {
      std::cerr << "FATAL: Caught an unexpected exception:\n" << ex.what() << std::endl;
    } catch (...) {
      std::cerr << "FATAL: Caught an unexpected and unknown exception." << std::endl;
    }
  } catch (...) {
    // In extremely rare cases, reporting could fail. Do another best effort attempt of writing to stderr
    detail::reportDiagnosticFailure();
  }
  return EXIT_FAILURE;
}

/**
 * Reports exceptions to the logger and returns EXIT_FAILURE, even if preparing the diagnostic throws.
 */
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRunWithLog(log::Logger& log, F&& func) noexcept {
  try {
    try {
      return func();
    } catch (const exception::UserError& ex) {
      log(log::CRIT,
          "Fatal user error",
          {
            {semconv::log::exceptionMessage, ex.getMessage().str()}
      });
    } catch (const exception::Exception& ex) {
      log(log::CRIT,
          "Fatal unexpected CTA exception",
          {
            {semconv::log::exceptionMessage, ex.getMessage().str()}
      });
    } catch (const std::exception& ex) {
      log(log::CRIT,
          "Fatal unexpected exception",
          {
            {semconv::log::exceptionMessage, ex.what()}
      });
    } catch (...) {
      log(log::CRIT, "Fatal unknown exception", {});
    }
  } catch (...) {
    // In extremely rare cases, reporting could fail. Do another best effort attempt of writing to stderr
    detail::reportDiagnosticFailure();
  }
  return EXIT_FAILURE;
}

}  // namespace cta::runtime
