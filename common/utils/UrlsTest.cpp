/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/utils/Urls.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(SplitURL, SplitsAllComponents) {
  const auto parts = cta::utils::splitURL("root://diskserver:1094//eos/file?cap.msg=secret#frag");
  ASSERT_TRUE(parts.has_value());
  const auto& [scheme, authority, path, query] = *parts;
  EXPECT_EQ("root", scheme);
  EXPECT_EQ("diskserver:1094", authority);
  EXPECT_EQ("//eos/file", path);
  EXPECT_EQ("cap.msg=secret", query);
}

TEST(SplitURL, HandlesMissingAuthorityAndQuery) {
  const auto parts = cta::utils::splitURL("null:");
  ASSERT_TRUE(parts.has_value());
  const auto& [scheme, authority, path, query] = *parts;
  EXPECT_EQ("null", scheme);
  EXPECT_EQ("", authority);
  EXPECT_EQ("", path);
  EXPECT_EQ("", query);
}

TEST(SplitURL, RejectsTextWithoutScheme) {
  EXPECT_FALSE(cta::utils::splitURL("/just/a/path").has_value());
}

}  // namespace unitTests
