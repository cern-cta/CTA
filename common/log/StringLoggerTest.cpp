/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StringLogger.hpp"

#include <gtest/gtest.h>

using namespace cta::log;

namespace unitTests {
TEST(cta_log_StringLogger, basicTest) {
  std::string jat = "Just a test";
  StringLogger sl("dummy", "cta_log_StringLogger", DEBUG);
  sl(INFO, jat);
  ASSERT_NE(std::string::npos, sl.getLog().find(jat));
}
}  // namespace unitTests
