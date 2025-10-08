/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Exception.hpp"

namespace cta::exception {

/**
 * class TimeoutException
 * A simple exception used for timeout handling in cts
 */
class TimeoutException : public Exception {
  using Exception::Exception;
};

} // namespace cta::exception
