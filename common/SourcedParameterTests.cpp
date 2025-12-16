/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/SourcedParameter.hpp"
#include "tapeserver/daemon/common/FetchReportOrFlushLimits.hpp"

#include <gtest/gtest.h>

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
