/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Regex.hpp"

#include <gmock/gmock-cardinalities.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using ::testing::_;
using ::testing::AtLeast;
using ::testing::Return;

namespace unitTests {

TEST(Regex, BasicFunctionality) {
  cta::utils::Regex re("a(b)");
  std::vector<std::string> ret1, ret2;
  ret1 = re.exec("1abc");
  ret2 = re.exec("xyz");

  ASSERT_EQ(ret1.size(), 2U);
  ASSERT_EQ(ret1[0], "ab");
  ASSERT_EQ(ret1[1], "b");
  ASSERT_EQ(ret2.size(), 0U);
}

TEST(Regex, OperationalTest) {
  cta::utils::Regex re("^scsi_tape:(st[[:digit:]]+)$");
  std::vector<std::string> ret1, ret2, ret3;
  ret1 = re.exec("scsi_tape:st1");
  ret2 = re.exec("scsi_tape:st124");
  ret3 = re.exec("scsi_tape:st1a");

  ASSERT_EQ(ret1.size(), 2U);
  ASSERT_EQ(ret1[0], "scsi_tape:st1");
  ASSERT_EQ(ret1[1], "st1");
  ASSERT_EQ(ret2.size(), 2U);
  ASSERT_EQ(ret2[0], "scsi_tape:st124");
  ASSERT_EQ(ret2[1], "st124");
  ASSERT_EQ(ret3.size(), 0U);
}

TEST(Regex, SubstringMatch) {
  cta::utils::Regex re("^radosstriper:///([^:]+@[^:]+):(.*)$");
  std::vector<std::string> ret1;
  ret1 = re.exec("radosstriper:///user@pool:12345@castorns.7890");

  ASSERT_EQ(ret1.size(), 3U);
  ASSERT_EQ(ret1[1], "user@pool");
  ASSERT_EQ(ret1[2], "12345@castorns.7890");
}

TEST(Regex, NestedMatch) {
  cta::utils::Regex re("^rados://([^@]+)@([^:]+)(|:(.+))$");
  auto ret1 = re.exec("rados://user@pool:namespace");

  ASSERT_EQ(5U, ret1.size());
  ASSERT_EQ("user", ret1[1]);
  ASSERT_EQ("pool", ret1[2]);
  ASSERT_EQ("namespace", ret1[4]);

  // The nested match does not show up in the result set if its branch is not met.
  ret1 = re.exec("rados://user1@pool2");
  ASSERT_EQ(4U, ret1.size());
  ASSERT_EQ("user1", ret1[1]);
  ASSERT_EQ("pool2", ret1[2]);
  ASSERT_EQ("", ret1[3]);
}

TEST(Regex, CopyConstructor) {
  cta::utils::Regex original("^rados://([^@]+)@([^:]+)(|:(.+))$");
  cta::utils::Regex copy(original);

  auto ret = copy.exec("rados://user@pool:namespace");

  ASSERT_EQ(5U, ret.size());
  ASSERT_EQ("user", ret[1]);
  ASSERT_EQ("pool", ret[2]);
  ASSERT_EQ("namespace", ret[4]);
}
}  // namespace unitTests
