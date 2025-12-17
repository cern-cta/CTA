/*
 * SPDX-FileCopyrightText: 2023 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Config.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <sstream>
#include <string>

namespace unitTests {

TEST(FrontendConfig, HandlesUInt) {
  TempFile tf;
  std::ostringstream strFile;

  strFile << "cta.val.a 1" << std::endl
          << "cta.val.b -10" << std::endl
          << "cta.val.c abc123" << std::endl
          << "cta.val.d 123abc" << std::endl
          << "cta.val.e abc123abc" << std::endl
          << "cta.val.f = 123" << std::endl
          << "cta.val.g " << std::numeric_limits<uint32_t>::max() << std::endl
          << "cta.val.h " << std::numeric_limits<uint32_t>::max() << "0" << std::endl
          << "cta.val.i " << std::numeric_limits<uint64_t>::max() << std::endl;

  tf.stringFill(strFile.str());

  cta::common::Config config(tf.path());

  EXPECT_EQ(1, config.getOptionValueUInt("cta.val.a"));
  EXPECT_THROW(config.getOptionValueUInt("cta.val.b"), std::out_of_range);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.c"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.d"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.e"), std::invalid_argument);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.f"), std::invalid_argument);
  EXPECT_EQ(std::numeric_limits<uint32_t>::max(), config.getOptionValueUInt("cta.val.g"));
  EXPECT_THROW(config.getOptionValueUInt("cta.val.h"), std::out_of_range);
  EXPECT_THROW(config.getOptionValueUInt("cta.val.i"), std::out_of_range);
}

}  // namespace unitTests
