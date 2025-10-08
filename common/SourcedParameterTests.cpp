/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gtest/gtest.h>

#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/common/FetchReportOrFlushLimits.hpp"

namespace unitTests {

TEST(cta_Daemon, SourcedParameter) {
  cta::SourcedParameter<cta::tape::daemon::common::FetchReportOrFlushLimits> mountCriteria("unitTest", "mountCriteria");
  mountCriteria.set("12, 34", "Unit test");
  ASSERT_EQ(12, mountCriteria.value().maxBytes);
  ASSERT_EQ(34, mountCriteria.value().maxFiles);
}

class SourcedParameterBoolTest : public ::testing::TestWithParam<std::pair<const char*, bool>> {};

INSTANTIATE_TEST_SUITE_P(ValidBooleanValues,
                         SourcedParameterBoolTest,
                         ::testing::Values(std::make_pair("true", true),
                                           std::make_pair("TRUE", true),
                                           std::make_pair("1", true),
                                           std::make_pair("yes", true),
                                           std::make_pair("YES", true),
                                           std::make_pair("false", false),
                                           std::make_pair("FALSE", false),
                                           std::make_pair("0", false),
                                           std::make_pair("no", false),
                                           std::make_pair("NO", false)));

TEST_P(SourcedParameterBoolTest, ValidValues) {
  cta::SourcedParameter<bool> param("unitTest", "boolKey");
  const auto [input, expected] = GetParam();

  param.set(input, "Unit test");
  ASSERT_EQ(expected, param.value());
  ASSERT_EQ("Unit test", param.source());
}

}  // namespace unitTests
