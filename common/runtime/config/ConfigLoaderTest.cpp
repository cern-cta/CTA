/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigLoader.hpp"

#include "common/runtime/RuntimeTestHelpers.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <limits>
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

  static constexpr std::size_t memberCount() { return 3; }
};

struct MapOnlyConfig {
  int mandatory;
  std::unordered_map<std::string, int> counts;

  static constexpr std::size_t memberCount() { return 2; }
};

struct Endpoint {
  std::string host;
  int port;

  static constexpr std::size_t memberCount() { return 2; }
};

struct Limits {
  int max_conn;
  std::vector<int> retry_backoff_ms;
  std::map<std::string, int> per_user;

  static constexpr std::size_t memberCount() { return 3; }
};

struct User {
  std::string name;
  std::vector<std::string> roles;
  std::optional<std::map<std::string, std::string>> labels = std::nullopt;

  static constexpr std::size_t memberCount() { return 3; }
};

struct ComplexConfig {
  int mandatory;
  std::optional<int> maybe = std::nullopt;
  std::string default_value = "default_value";

  Endpoint endpoint;
  Limits limits;

  std::vector<User> users;
  std::map<std::string, std::vector<int>> matrix;
  std::vector<std::vector<int>> nested_arrays;
  std::optional<std::vector<std::map<std::string, int>>> maybe_tables = std::nullopt;

  static constexpr std::size_t memberCount() { return 9; }
};

struct ErrorMessageConfig {
  int mandatory;
  std::optional<int> maybe = std::nullopt;
  std::string default_value = "default_value";
  Endpoint endpoint;
  Limits limits;
  std::vector<User> users;
  std::map<std::string, std::vector<int>> matrix;
  std::vector<std::vector<int>> nested_arrays;
  std::optional<std::vector<std::map<std::string, int>>> maybe_tables = std::nullopt;
  bool boolean = false;
  double floating = 0.0;
  std::uint8_t narrow = 0;
  std::string text;

  static constexpr std::size_t memberCount() { return 13; }
};

struct IntegerConfig {
  std::int8_t signed_8 = 0;
  std::uint8_t unsigned_8 = 0;

  static constexpr std::size_t memberCount() { return 2; }
};

struct ScalarConfig {
  bool boolean = false;
  int integer = 0;

  static constexpr std::size_t memberCount() { return 2; }
};

struct FloatingPointConfig {
  float single_precision = 0.0F;
  double double_precision = 0.0;

  static constexpr std::size_t memberCount() { return 2; }
};

struct LongLongConfig {
  long long value = 0;

  static constexpr std::size_t memberCount() { return 1; }
};

struct OptionalAggregateConfig {
  std::optional<Endpoint> endpoint = std::nullopt;

  static constexpr std::size_t memberCount() { return 1; }
};

//------------------------------------------------------------------------------
// Tests
//------------------------------------------------------------------------------

TEST(ConfigLoader, ThrowsOnNonExistingPath) {
  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>("/tmp/idefinitelydontexist.toml", false)),
               cta::exception::UserError);
}

TEST(ConfigLoader, AllowsIntegerValuesAtDestinationTypeBounds) {
  TempFile f(R"toml(
signed_8 = -128
unsigned_8 = 255
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<IntegerConfig>(f.path());
  EXPECT_EQ(config.signed_8, std::numeric_limits<std::int8_t>::min());
  EXPECT_EQ(config.unsigned_8, std::numeric_limits<std::uint8_t>::max());
}

TEST(ConfigLoader, ThrowsOnNegativeIntegerForUnsignedDestination) {
  TempFile f("unsigned_8 = -1\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<IntegerConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerBelowDestinationTypeMinimum) {
  TempFile f("signed_8 = -129\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<IntegerConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerAboveSignedDestinationTypeMaximum) {
  TempFile f("signed_8 = 128\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<IntegerConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerAboveUnsignedDestinationTypeMaximum) {
  TempFile f("unsigned_8 = 256\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<IntegerConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerForBooleanDestination) {
  TempFile f("boolean = 2\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<ScalarConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnBooleanForIntegerDestination) {
  TempFile f("integer = true\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<ScalarConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnFloatingPointForIntegerDestination) {
  TempFile f("integer = 7.0\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<ScalarConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, AllowsFloatingPointValuesForFloatingPointDestinations) {
  TempFile f("single_precision = 1.5\ndouble_precision = -2.25\n", ".toml");

  const auto config = cta::runtime::loadFromToml<FloatingPointConfig>(f.path());
  EXPECT_FLOAT_EQ(config.single_precision, 1.5F);
  EXPECT_DOUBLE_EQ(config.double_precision, -2.25);
}

TEST(ConfigLoader, ThrowsOnFloatingPointAboveDestinationTypeMaximum) {
  TempFile f("single_precision = 3.5e38\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<FloatingPointConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerForFloatingPointDestination) {
  TempFile f("double_precision = 7\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<FloatingPointConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, AllowsLongLongValuesAtTomlIntegerBounds) {
  TempFile minFile("value = -9223372036854775808\n", ".toml");
  TempFile maxFile("value = 9223372036854775807\n", ".toml");

  EXPECT_EQ(cta::runtime::loadFromToml<LongLongConfig>(minFile.path()).value, std::numeric_limits<long long>::min());
  EXPECT_EQ(cta::runtime::loadFromToml<LongLongConfig>(maxFile.path()).value, std::numeric_limits<long long>::max());
}

TEST(ConfigLoader, ThrowsOnIntegerBelowLongLongMinimum) {
  TempFile f("value = -9223372036854775809\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<LongLongConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ThrowsOnIntegerAboveLongLongMaximum) {
  TempFile f("value = 9223372036854775808\n", ".toml");
  EXPECT_THROW(cta::runtime::loadFromToml<LongLongConfig>(f.path()), cta::exception::UserError);
}

TEST(ConfigLoader, ValueInitializesMissingFieldsInOptionalNestedAggregate) {
  TempFile f(R"toml(
[endpoint]
host = "localhost"
)toml",
             ".toml");

  const auto config = cta::runtime::loadFromToml<OptionalAggregateConfig>(f.path());
  ASSERT_TRUE(config.endpoint.has_value());
  EXPECT_EQ(config.endpoint->host, "localhost");
  EXPECT_EQ(config.endpoint->port, 0);
}

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
# missing equals sign
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

TEST(ConfigLoader, LenientAllowsUnorderedMap) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = 1, bob = 2, "svc-account" = 3 }
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), false);
  ASSERT_EQ(cfg.mandatory, 7);
  ASSERT_EQ(cfg.counts.size(), 3U);
  ASSERT_EQ(cfg.counts.at("alice"), 1);
  ASSERT_EQ(cfg.counts.at("bob"), 2);
  ASSERT_EQ(cfg.counts.at("svc-account"), 3);
}

TEST(ConfigLoader, LenientAllowsUnorderedMapEmpty) {
  TempFile f(R"toml(
mandatory = 7
counts = {}
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), false);
  ASSERT_EQ(cfg.mandatory, 7);
  ASSERT_TRUE(cfg.counts.empty());
}

TEST(ConfigLoader, LenientThrowsOnUnorderedMapWrongValueType) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = "one" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnUnorderedMapWrongShapeArrayInsteadOfTable) {
  TempFile f(R"toml(
mandatory = 7
# Supposed to be a map
counts = [1, 2, 3]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientAllowsNestedTablesArraysAndInlineTables) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1, 2, 3], b = [4, 5] }
nested_arrays = [[1, 2], [3, 4, 5]]
maybe_tables = [{ k = 1, v = 2 }, { k = 3, v = 4 }]

[endpoint]
host = "localhost"
port = 8080
unknown_endpoint_key = "ignored in lenient"

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5, bob = 10, "svc-account" = 1 }
extra_limits_key = 999

[[users]]
name = "alice"
roles = ["admin", "user"]
labels = { team = "core", site = "cern" }
unknown_user_key = true

[[users]]
name = "bob"
roles = ["user"]
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<ComplexConfig>(f.path(), false);

  ASSERT_EQ(cfg.mandatory, 7);
  ASSERT_TRUE(cfg.maybe.has_value());
  ASSERT_EQ(cfg.maybe.value(), 123);
  ASSERT_EQ(cfg.default_value, "hi");

  ASSERT_EQ(cfg.endpoint.host, "localhost");
  ASSERT_EQ(cfg.endpoint.port, 8080);

  ASSERT_EQ(cfg.limits.max_conn, 100);
  ASSERT_EQ(cfg.limits.retry_backoff_ms.size(), 3U);
  ASSERT_EQ(cfg.limits.retry_backoff_ms.at(0), 10);
  ASSERT_EQ(cfg.limits.retry_backoff_ms.at(1), 20);
  ASSERT_EQ(cfg.limits.retry_backoff_ms.at(2), 50);
  ASSERT_EQ(cfg.limits.per_user.at("alice"), 5);
  ASSERT_EQ(cfg.limits.per_user.at("bob"), 10);
  ASSERT_EQ(cfg.limits.per_user.at("svc-account"), 1);

  ASSERT_EQ(cfg.users.size(), 2U);
  ASSERT_EQ(cfg.users.at(0).name, "alice");
  ASSERT_EQ(cfg.users.at(0).roles.size(), 2U);
  ASSERT_EQ(cfg.users.at(0).roles.at(0), "admin");
  ASSERT_EQ(cfg.users.at(0).roles.at(1), "user");
  ASSERT_TRUE(cfg.users.at(0).labels.has_value());
  ASSERT_EQ(cfg.users.at(0).labels->at("team"), "core");
  ASSERT_EQ(cfg.users.at(0).labels->at("site"), "cern");

  ASSERT_EQ(cfg.users.at(1).name, "bob");
  ASSERT_EQ(cfg.users.at(1).roles.size(), 1U);
  ASSERT_EQ(cfg.users.at(1).roles.at(0), "user");
  ASSERT_FALSE(cfg.users.at(1).labels.has_value());

  ASSERT_EQ(cfg.matrix.at("a").size(), 3U);
  ASSERT_EQ(cfg.matrix.at("a").at(0), 1);
  ASSERT_EQ(cfg.matrix.at("a").at(1), 2);
  ASSERT_EQ(cfg.matrix.at("a").at(2), 3);
  ASSERT_EQ(cfg.matrix.at("b").size(), 2U);
  ASSERT_EQ(cfg.matrix.at("b").at(0), 4);
  ASSERT_EQ(cfg.matrix.at("b").at(1), 5);

  ASSERT_EQ(cfg.nested_arrays.size(), 2U);
  ASSERT_EQ(cfg.nested_arrays.at(0).size(), 2U);
  ASSERT_EQ(cfg.nested_arrays.at(0).at(0), 1);
  ASSERT_EQ(cfg.nested_arrays.at(0).at(1), 2);
  ASSERT_EQ(cfg.nested_arrays.at(1).size(), 3U);
  ASSERT_EQ(cfg.nested_arrays.at(1).at(0), 3);
  ASSERT_EQ(cfg.nested_arrays.at(1).at(1), 4);
  ASSERT_EQ(cfg.nested_arrays.at(1).at(2), 5);

  ASSERT_TRUE(cfg.maybe_tables.has_value());
  ASSERT_EQ(cfg.maybe_tables->size(), 2U);
  ASSERT_EQ(cfg.maybe_tables->at(0).at("k"), 1);
  ASSERT_EQ(cfg.maybe_tables->at(0).at("v"), 2);
  ASSERT_EQ(cfg.maybe_tables->at(1).at("k"), 3);
  ASSERT_EQ(cfg.maybe_tables->at(1).at("v"), 4);
}

TEST(ConfigLoader, LenientAllowsMissingNestedOptionalAndUsesTopLevelDefault) {
  TempFile f(R"toml(
mandatory = 7

matrix = { a = [1], b = [2, 3] }
nested_arrays = [[1], [2, 3]]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<ComplexConfig>(f.path(), false);

  ASSERT_EQ(cfg.mandatory, 7);
  ASSERT_FALSE(cfg.maybe.has_value());
  ASSERT_EQ(cfg.default_value, "default_value");
  ASSERT_FALSE(cfg.maybe_tables.has_value());
  ASSERT_EQ(cfg.users.size(), 1U);
  ASSERT_FALSE(cfg.users.at(0).labels.has_value());
}

TEST(ConfigLoader, LenientThrowsOnDeepIncorrectType) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = "five" }

[[users]]
name = "alice"
roles = ["admin"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnDuplicateTopLevelKey) {
  TempFile f(R"toml(
mandatory = 7
mandatory = 8
maybe = 1
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnDuplicateKeyInInlineTable) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = 1, alice = 2 }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnMixedTypeArray) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, "20", 30]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnMixedNestedArrays) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1, 2], ["x"]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientThrowsOnArrayOfTablesThenTableShapeChange) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]

[users]
name = "bob"
roles = ["user"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), false)), cta::exception::UserError);
}

TEST(ConfigLoader, LenientErrorMessages) {
  TempFile f(R"toml(
mandatory = 7
# maybe should be an int, not a string
maybe = "123"
default_value = "hi"
boolean = 1
floating = true
narrow = 256
text = 1

# a is supposed to be a list
matrix = { a = "hello" }
nested_arrays = [[1, 2], [3, 4, 5]]

maybe_tables = [{ k = 1, v = 2 }, { k = 3, v = 4 }]

# This key doesn't exist in the config
idontexist = 3

[endpoint]
host = "localhost"
port = 8080

[limits]
# Should be an int
max_conn = "100"
# Should be a list
retry_backoff_ms = 20
# Subtype should not be a string
per_user = { alice = "5", bob = 10, "svc-account" = 1 }

# Missing users table
)toml",
             ".toml");

  try {
    cta::runtime::loadFromToml<ErrorMessageConfig>(f.path(), false);
    FAIL() << "Expected cta::exception::UserError";
  } catch (const cta::exception::UserError& e) {
    std::string expectedErrorMessage = "Invalid config in '" + f.path() + R"""(':
1) Field 'boolean' must be a boolean.
2) Field 'floating' must be a floating-point number.
3) Field 'narrow' is outside the supported range.
4) Field 'text' has an invalid value or type.
5) Failed to parse field 'limits':
    1) Field 'max_conn' must be an integer.
    2) Field 'retry_backoff_ms' must be an array.
    3) Failed to parse field 'per_user':
        1) Field 'alice' must be an integer.
6) Failed to parse field 'matrix':
    1) Field 'a' must be an array.
7) Failed to parse field 'maybe':
    1) Field 'maybe' must be an integer.

)""";
    EXPECT_EQ(std::string(e.what()), expectedErrorMessage);
  } catch (...) {
    FAIL() << "Expected cta::exception::UserError";
  }
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

TEST(ConfigLoader, StrictAllowsUnorderedMap) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = 1, bob = 2, "svc-account" = 3 }
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), true);
  ASSERT_EQ(cfg.mandatory, 7);
  ASSERT_EQ(cfg.counts.size(), 3U);
  ASSERT_EQ(cfg.counts.at("alice"), 1);
  ASSERT_EQ(cfg.counts.at("bob"), 2);
  ASSERT_EQ(cfg.counts.at("svc-account"), 3);
}

TEST(ConfigLoader, StrictThrowsOnMissingUnorderedMap) {
  TempFile f(R"toml(
mandatory = 7
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnUnorderedMapWrongValueType) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = "one" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnUnknownKeyAlongsideUnorderedMap) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = 1 }
unknown = 123
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictAllowsNestedTablesArraysAndInlineTables) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1, 2, 3], b = [4, 5] }
nested_arrays = [[1, 2], [3, 4, 5]]

maybe_tables = [{ k = 1, v = 2 }, { k = 3, v = 4 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5, bob = 10, "svc-account" = 1 }

[[users]]
name = "alice"
roles = ["admin", "user"]
labels = { team = "core", site = "cern" }

[[users]]
name = "bob"
roles = ["user"]
labels = { team = "ops" }
)toml",
             ".toml");

  const auto cfg = cta::runtime::loadFromToml<ComplexConfig>(f.path(), true);
  ASSERT_EQ(cfg.endpoint.host, "localhost");
  ASSERT_EQ(cfg.limits.per_user.at("bob"), 10);
  ASSERT_EQ(cfg.users.size(), 2U);
  ASSERT_TRUE(cfg.users.at(0).labels.has_value());
  ASSERT_TRUE(cfg.maybe_tables.has_value());
}

TEST(ConfigLoader, StrictThrowsOnUnknownKeyInNestedTable) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080
extra = "nope"

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
labels = { team = "core" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingNestedValue) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
# port missing

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
labels = { team = "core" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingArrayOfTablesEntryField) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5 }

[[users]]
# name missing
roles = ["admin"]
labels = { team = "core" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMissingOptionalAndDefaultAtTopLevel) {
  TempFile f(R"toml(
mandatory = 7
# maybe missing
# default_value missing

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
labels = { team = "core" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnDeepIncorrectType) {
  TempFile f(R"toml(
mandatory = 7
maybe = 123
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20, 50]
per_user = { alice = "five" }

[[users]]
name = "alice"
roles = ["admin"]
labels = { team = "core" }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnDuplicateTopLevelKey) {
  TempFile f(R"toml(
mandatory = 7
mandatory = 8
maybe = 1
default_value = "hi"
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnDuplicateKeyInInlineTable) {
  TempFile f(R"toml(
mandatory = 7
counts = { alice = 1, alice = 2 }
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<MapOnlyConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMixedTypeArray) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, "20", 30]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnMixedNestedArrays) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1, 2], ["x"]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]

)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictThrowsOnArrayOfTablesThenTableShapeChange) {
  TempFile f(R"toml(
mandatory = 7
maybe = 1
default_value = "hi"

matrix = { a = [1], b = [2] }
nested_arrays = [[1], [2]]
maybe_tables = [{ k = 1, v = 2 }]

[endpoint]
host = "localhost"
port = 8080

[limits]
max_conn = 100
retry_backoff_ms = [10, 20]
per_user = { alice = 5 }

[[users]]
name = "alice"
roles = ["admin"]

[users]
name = "bob"
roles = ["user"]
)toml",
             ".toml");

  EXPECT_THROW((cta::runtime::loadFromToml<ComplexConfig>(f.path(), true)), cta::exception::UserError);
}

TEST(ConfigLoader, StrictErrorMessages) {
  TempFile f(R"toml(
mandatory = 7
# maybe should be an int, not a string
maybe = "123"
default_value = "hi"
boolean = 1
floating = true
narrow = 256
text = 1

# a is supposed to be a list
matrix = { a = "hello" }
nested_arrays = [[1, 2], [3, 4, 5]]

maybe_tables = [{ k = 1, v = 2 }, { k = 3, v = 4 }]

# This key doesn't exist in the config
idontexist = 3

[endpoint]
host = "localhost"
port = 8080

[limits]
# Should be an int
max_conn = "100"
# Should be a list
retry_backoff_ms = 20
# Subtype should not be a string
per_user = { alice = "5", bob = 10, "svc-account" = 1 }

# Missing users table
)toml",
             ".toml");

  try {
    cta::runtime::loadFromToml<ErrorMessageConfig>(f.path(), true);
    FAIL() << "Expected cta::exception::UserError";
  } catch (const cta::exception::UserError& e) {
    std::string expectedErrorMessage = "Invalid config in '" + f.path() + R"""(':
1) Expected field 'users' is missing.
2) Field 'boolean' must be a boolean.
3) Field 'floating' must be a floating-point number.
4) Field 'narrow' is outside the supported range.
5) Field 'text' has an invalid value or type.
6) Unknown field 'idontexist'.
7) Failed to parse field 'limits':
    1) Field 'max_conn' must be an integer.
    2) Field 'retry_backoff_ms' must be an array.
    3) Failed to parse field 'per_user':
        1) Field 'alice' must be an integer.
8) Failed to parse field 'matrix':
    1) Field 'a' must be an array.
9) Failed to parse field 'maybe':
    1) Field 'maybe' must be an integer.

)""";
    EXPECT_EQ(std::string(e.what()), expectedErrorMessage);
  } catch (...) {
    FAIL() << "Expected cta::exception::UserError";
  }
}

}  // namespace unitTests
