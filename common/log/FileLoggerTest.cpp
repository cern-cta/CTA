/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FileLogger.hpp"

#include "tests/TempFile.hpp"

#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using namespace cta::log;

namespace unitTests {
TEST(cta_log_FileLogger, basicTest) {
  std::string jat = "Just a test";
  TempFile tf;
  FileLogger fl("dummy", "cta_log_StringLogger", tf.path(), DEBUG);
  fl(INFO, jat);
  std::ifstream ifs(tf.path());
  std::stringstream res;
  res << ifs.rdbuf();
  ASSERT_NE(std::string::npos, res.str().find(jat));
}
}  // namespace unitTests
