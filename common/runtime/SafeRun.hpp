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

/**
 * Wraps the provided function in a try/catch and reports exceptions to stderr.
 */
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRun(F&& func) {
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
  return EXIT_FAILURE;
}

/**
 * Wraps the provided function in a try/catch and reports exceptions to the logging system.
 */
template<typename F>
  requires std::invocable<F> && std::convertible_to<std::invoke_result_t<F>, int>
int safeRunWithLog(log::Logger& log, F&& func) {
  try {
    return func();
  } catch (const exception::UserError& ex) {
    log(log::CRIT,
        "FATAL: User Error",
        {
          {semconv::log::exceptionMessage, ex.getMessage().str()}
    });
  } catch (const exception::Exception& ex) {
    log(log::CRIT,
        "FATAL: Caught an unexpected CTA exception. Stack trace follows.",
        {
          {semconv::log::exceptionMessage, ex.getMessage().str()}
    });
  } catch (const std::exception& ex) {
    log(log::CRIT,
        "FATAL: Caught an unexpected exception",
        {
          {semconv::log::exceptionMessage, ex.what()}
    });
  } catch (...) {
    log(log::CRIT, "FATAL: Caught an unexpected and unknown exception", {});
  }
  sleep(1);
  return EXIT_FAILURE;
}

}  // namespace cta::runtime
