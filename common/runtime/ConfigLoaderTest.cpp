/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigLoader.hpp"

#include "RuntimeTestHelpers.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <utility>

namespace unitTests {

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

struct MyConfig {
  int mandatory;
  std::optional<int> maybe = std::nullopt;
  std::string default_value = "default_value";
};

enum class CustomFoodEnum { SPAGHETTI, PIZZA, BROODJE_PINDAKAAS };

struct MyEnumConfig {
  CustomFoodEnum food;
};

//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

// Lenient (default) Mode

TEST(ConfigLoader, LenientThrowsOnGarbageToml) {
  TempFile f(R"toml(
garbage
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnIncorrectToml) {
  TempFile f(R"toml(
# Oh no, forgot an equals sign
mandatory 7
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientAllowsAllValues) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, LenientAllowsMissingOptionals) {
  TempFile f(R"toml(
mandatory = 7
# maybe missing
default_value = "hi"
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_FALSE(config.maybe.has_value());
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, LenientAllowsMissingDefaults) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
# default_value missing
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "default_value");
}

TEST(ConfigLoader, LenientAllowsUnknownKeys) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
unknown_key = "ignored in lenient"
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, LenientThrowsOnIncorrectType) {
  TempFile f(R"toml(
mandatory = "I am not supposed to be a string"
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientHandlesEnum) {
  TempFile f(R"toml(
food = PIZZA
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyEnumConfig>(f.path(), false);
  ASSERT_EQ(config.food, CustomFoodEnum::PIZZA);
}

TEST(ConfigLoader, LenientThrowsOnInvalidEnum) {
  TempFile f(R"toml(
food = STOEPTEGEL # A STOEPTEGEL is not food obviously
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyEnumConfig>(f.path(), false)), cta::exception::UserError);
}

// Strict Mode

TEST(ConfigLoader, StrictThrowsOnGarbageToml) {
  TempFile f(R"toml(
garbage
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnIncorrectToml) {
  TempFile f(R"toml(
# Oh no, forgot an equals sign
mandatory 7
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictAllowsAllValues) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyConfig>(f.path(), true);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, StrictThrowsOnMissingMandatoryValue) {
  TempFile f(R"toml(
# mandatory missing
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingOptionals) {
  TempFile f(R"toml(
mandatory = 7
# maybe missing
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingDefaults) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
# default_value missing
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnUnknownKeys) {
  TempFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
unknown_key = "ignored in lenient"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnIncorrectType) {
  TempFile f(R"toml(
mandatory = "I am not supposed to be a string"
maybe = 42
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictHandlesEnum) {
  TempFile f(R"toml(
food = PIZZA
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<MyEnumConfig>(f.path(), true);
  ASSERT_EQ(config.food, CustomFoodEnum::PIZZA);
}

TEST(ConfigLoader, StrictThrowsOnInvalidEnum) {
  TempFile f(R"toml(
food = STOEPTEGEL # A STOEPTEGEL is not food obviously
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyEnumConfig>(f.path(), true)), cta::exception::UserError);
}

}  // namespace unitTests
