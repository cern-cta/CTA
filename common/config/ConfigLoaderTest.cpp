/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigLoader.hpp"

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

class TempTomlFile {
public:
  explicit TempTomlFile(const std::string& content) {
    auto tmpl = std::filesystem::temp_directory_path() / "rfl_test_XXXXXX.toml";
    std::string s = tmpl.string();

    int fd = ::mkstemp(s.data());
    if (fd == -1) {
      throw std::runtime_error("mkstemp failed");
    }

    m_path = s;
    std::ofstream out(m_path, std::ios::binary);
    out << content;
    ::close(fd);
  }

  ~TempTomlFile() {
    std::error_code ec;
    std::filesystem::remove(m_path, ec);
  }

  const std::filesystem::path& path() const { return m_path; }

private:
  std::filesystem::path m_path;
};

struct MyConfig {
  int mandatory;
  std::optional<int> maybe;
  std::string default_value = "default_value";
};

//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

// Lenient (default) Mode

TEST(ConfigLoader, LenientThrowsOnGarbageToml) {
  TempTomlFile f(R"toml(
garbage
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnIncorrectToml) {
  TempTomlFile f(R"toml(
# Oh no, forgot an equals sign
mandatory 7
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientAllowsAllValues) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
)toml");

  const auto config = cta::config::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, LenientThrowsOnMissingMandatoryValue) {
  TempTomlFile f(R"toml(
# mandatory missing
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientAllowsMissingOptionals) {
  TempTomlFile f(R"toml(
mandatory = 7
# maybe missing
default_value = "hi"
)toml");

  const auto config = cta::config::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_FALSE(config.maybe.has_value());
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, LenientAllowsMissingDefaults) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
# default_value missing
)toml");

  const auto config = cta::config::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "default_value");
}

TEST(ConfigLoader, LenientAllowsUnknownKeys) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
unknown_key = "ignored in lenient"
)toml");

  const auto config = cta::config::loadFromToml<MyConfig>(f.path(), false);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "default_value");
}

TEST(ConfigLoader, LenientThrowsOnIncorrectType) {
  TempTomlFile f(R"toml(
mandatory = "I am not supposed to be a string"
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

// Strict Mode

TEST(ConfigLoader, StrictThrowsOnGarbageToml) {
  TempTomlFile f(R"toml(
garbage
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnIncorrectToml) {
  TempTomlFile f(R"toml(
# Oh no, forgot an equals sign
mandatory 7
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictAllowsAllValues) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
)toml");

  const auto config = cta::config::loadFromToml<MyConfig>(f.path(), true);
  ASSERT_EQ(config.mandatory, 7);
  ASSERT_TRUE(config.maybe.has_value());
  ASSERT_EQ(config.maybe.value(), 42);
  ASSERT_EQ(config.default_value, "hi");
}

TEST(ConfigLoader, DefaultThrowsOnMissingMandatoryValue) {
  TempTomlFile f(R"toml(
# mandatory missing
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingOptionals) {
  TempTomlFile f(R"toml(
mandatory = 7
# maybe missing
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingDefaults) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
# default_value missing
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnUnknownKeys) {
  TempTomlFile f(R"toml(
mandatory = 7
maybe = 42
default_value = "hi"
unknown_key = "ignored in lenient"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnIncorrectType) {
  TempTomlFile f(R"toml(
mandatory = "I am not supposed to be a string"
maybe = 42
default_value = "hi"
)toml");

  EXPECT_THROW((cta::config::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

}  // namespace unitTests
