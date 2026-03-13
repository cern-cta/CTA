/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Reflection.hpp"

#include <gtest/gtest.h>
#include <optional>
#include <string>

namespace unitTests {

TEST(Reflection, MemberNamesCorrect) {
  struct MyConfig {
    int mandatory;
    std::optional<int> maybe = std::nullopt;
    std::string default_value = "default_value";

    static constexpr std::size_t memberCount() { return 3; }
  };

  const auto memberNames = cta::runtime::parsing::reflection::getMemberNames<MyConfig, MyConfig::memberCount()>();
  ASSERT_EQ(memberNames[0], "mandatory");
  ASSERT_EQ(memberNames[1], "maybe");
  ASSERT_EQ(memberNames[2], "default_value");
}

TEST(Reflection, ModifiesMembersCorrectly) {
  struct StringConfig {
    std::string field1 = "contents1";
    std::string field2 = "contents2";
    std::string field3 = "contents3";

    static constexpr std::size_t memberCount() { return 3; }
  };

  StringConfig config;

  // First verify that we can loop over it with non reference
  // In this case, modifying shouldn't do anything
  cta::runtime::parsing::reflection::forEachMember(config, [](std::string_view fieldName, auto field) {
    ASSERT_TRUE(fieldName.starts_with("field"));
    field = "modified";
  });

  ASSERT_EQ(config.field1, "contents1");
  ASSERT_EQ(config.field2, "contents2");
  ASSERT_EQ(config.field3, "contents3");

  // Now by reference, so the modification should take effect.
  cta::runtime::parsing::reflection::forEachMember(config, [](std::string_view fieldName, auto& field) {
    if (fieldName == "field1") {
      field = "modified1";
    } else if (fieldName == "field2") {
      field = "modified2";
    } else if (fieldName == "field3") {
      field = "modified3";
    } else {
      FAIL() << "We shouldn't get here.";
    }
  });

  ASSERT_EQ(config.field1, "modified1");
  ASSERT_EQ(config.field2, "modified2");
  ASSERT_EQ(config.field3, "modified3");
}

// Ideally we have some tests what happens if memberCount is missing or incorrect, but these cases don't compile, so we can't..

}  // namespace unitTests
