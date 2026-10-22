/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/config/EnvironmentSubstitution.hpp"

#include "common/exception/UserError.hpp"

#include <cstdlib>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace unitTests {
namespace {

using cta::runtime::substituteEnvironmentVariables;

class EnvironmentSubstitution : public ::testing::Test {
protected:
  void SetUp() override {
    // Save and restore every touched variable, including ones originally absent or empty.
    const std::vector<std::pair<std::string, std::optional<std::string>>> variables {
      {"STRING_VALUE",      "value"               },
      {"BOOL_VALUE",        "true"                },
      {"INT_VALUE",         "1"                   },
      {"FLOAT_VALUE",       "1.1"                 },
      {"HEX_VALUE",         "0xdeadbeef"          },
      {"INVALID_MAP_VALUE", "value\nkey:value"    },
      {"DO_NOT_REPLACE_ME", "Never use this value"},
      {"REPLACE_ME",        "${DO_NOT_REPLACE_ME}"},
      {"VALUE_WITH_ESCAPE", "value$$"             },
      {"EMPTY_VALUE",       ""                    },
      {"UNDEFINED_KEY",     std::nullopt          },
      {"env",               std::nullopt          },
      {"_cta_Test9",        "mixed"               },
      {"QUOTED_VALUE",      "a\"b\\c\n\t"         },
    };
    for (const auto& [name, value] : variables) {
      const char* previous = std::getenv(name.c_str());
      m_saved.emplace_back(name, previous ? std::optional<std::string>(previous) : std::nullopt);
      ASSERT_EQ(value ? ::setenv(name.c_str(), value->c_str(), 1) : ::unsetenv(name.c_str()), 0);
    }
  }

  void TearDown() override {
    for (const auto& [name, value] : m_saved) {
      EXPECT_EQ(value ? ::setenv(name.c_str(), value->c_str(), 1) : ::unsetenv(name.c_str()), 0);
    }
  }

private:
  std::vector<std::pair<std::string, std::optional<std::string>>> m_saved;
};

TEST_F(EnvironmentSubstitution, OpenTelemetrySpecificationExamples) {
  // Scalar inputs and outputs from the specification's examples; node typing belongs to the parser.
  const std::vector<std::pair<std::string, std::string>> examples {
    {"${STRING_VALUE}",                     "value"                             },
    {"${BOOL_VALUE}",                       "true"                              },
    {"${INT_VALUE}",                        "1"                                 },
    {"${FLOAT_VALUE}",                      "1.1"                               },
    {"${HEX_VALUE}",                        "0xdeadbeef"                        },
    {"${env:STRING_VALUE}",                 "value"                             },
    {"${INVALID_MAP_VALUE}",                "value\nkey:value"                  },
    {"foo ${STRING_VALUE} ${FLOAT_VALUE}",  "foo value 1.1"                     },
    {"${UNDEFINED_KEY}",                    ""                                  },
    {"${UNDEFINED_KEY:-fallback}",          "fallback"                          },
    {"${REPLACE_ME}",                       "${DO_NOT_REPLACE_ME}"              },
    {"${UNDEFINED_KEY:-${STRING_VALUE}}",   "${STRING_VALUE}"                   },
    {"$${STRING_VALUE}",                    "${STRING_VALUE}"                   },
    {"$$${STRING_VALUE}",                   "$value"                            },
    {"$$$${STRING_VALUE}",                  "$${STRING_VALUE}"                  },
    {"$${STRING_VALUE:-fallback}",          "${STRING_VALUE:-fallback}"         },
    {"$${STRING_VALUE:-${STRING_VALUE}}",   "${STRING_VALUE:-value}"            },
    {"${UNDEFINED_KEY:-$${UNDEFINED_KEY}}", "${UNDEFINED_KEY:-${UNDEFINED_KEY}}"},
    {"${VALUE_WITH_ESCAPE}",                "value$$"                           },
    {"a $$ b",                              "a $ b"                             },
    {"a $ b",                               "a $ b"                             },
  };
  for (const auto& [input, expected] : examples) {
    SCOPED_TRACE(input);
    EXPECT_EQ(substituteEnvironmentVariables(input), expected);
  }
}

TEST_F(EnvironmentSubstitution, PreservesOrdinaryTextAndBareVariables) {
  for (const std::string input : {"", "ordinary text", "$STRING_VALUE", "$", "a\nb\tc", "héllo"}) {
    SCOPED_TRACE(input);
    EXPECT_EQ(substituteEnvironmentVariables(input, true), input);
  }
}

TEST_F(EnvironmentSubstitution, SupportsNamesAndAdjacentReferences) {
  EXPECT_EQ(substituteEnvironmentVariables("${_cta_Test9}:${STRING_VALUE}${env:INT_VALUE}"), "mixed:value1");
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE}/${STRING_VALUE}"), "value/value");
}

TEST_F(EnvironmentSubstitution, UndefinedAndEmptyVariablesResolveToEmptyByDefault) {
  EXPECT_EQ(substituteEnvironmentVariables("a${UNDEFINED_KEY}b${env:UNDEFINED_KEY}c"), "abc");
  EXPECT_EQ(substituteEnvironmentVariables("${EMPTY_VALUE}${env:EMPTY_VALUE}"), "");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY}", false), "");
}

TEST_F(EnvironmentSubstitution, MissingVariableErrorsAreOptionalAndIdentifyTheName) {
  for (const auto* input : {"${UNDEFINED_KEY}", "${env:UNDEFINED_KEY}"}) {
    SCOPED_TRACE(input);
    try {
      substituteEnvironmentVariables(input, true);
      FAIL() << "Expected a missing-variable error";
    } catch (const cta::exception::UserError& e) {
      EXPECT_NE(e.getMessageValue().find("UNDEFINED_KEY"), std::string::npos);
    }
  }
  EXPECT_EQ(substituteEnvironmentVariables("${EMPTY_VALUE}${env:EMPTY_VALUE}", true), "");
}

TEST_F(EnvironmentSubstitution, FallbacksApplyToUndefinedAndEmptyVariablesInBothModes) {
  for (const bool errorOnMissing : {false, true}) {
    for (const auto* name : {"UNDEFINED_KEY", "EMPTY_VALUE", "env:UNDEFINED_KEY", "env:EMPTY_VALUE"}) {
      SCOPED_TRACE(name);
      EXPECT_EQ(substituteEnvironmentVariables("${" + std::string(name) + ":-fallback}", errorOnMissing), "fallback");
      EXPECT_EQ(substituteEnvironmentVariables("${" + std::string(name) + ":-}", errorOnMissing), "");
    }
  }
}

TEST_F(EnvironmentSubstitution, EnvIsAlsoAValidVariableName) {
  EXPECT_EQ(substituteEnvironmentVariables("${env:-fallback}", true), "fallback");
  EXPECT_EQ(substituteEnvironmentVariables("${env:env:-fallback}", true), "fallback");
  ASSERT_EQ(::setenv("env", "present", 1), 0);
  EXPECT_EQ(substituteEnvironmentVariables("${env} ${env:-fallback} ${env:env:-fallback}"), "present present present");
}

TEST_F(EnvironmentSubstitution, NonemptyValuesTakePrecedenceOverFallbacks) {
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE:-fallback}"), "value");
  EXPECT_EQ(substituteEnvironmentVariables("${env:STRING_VALUE:-fallback}", true), "value");
}

TEST_F(EnvironmentSubstitution, FalseLookingAndWhitespaceValuesOverrideFallbacks) {
  for (const auto* value : {"0", "false", " ", "\t\n"}) {
    SCOPED_TRACE(value);
    ASSERT_EQ(::setenv("STRING_VALUE", value, 1), 0);
    for (const bool errorOnMissing : {false, true}) {
      EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE:-fallback}", errorOnMissing), value);
      EXPECT_EQ(substituteEnvironmentVariables("${env:STRING_VALUE:-fallback}", errorOnMissing), value);
    }
  }
}

TEST_F(EnvironmentSubstitution, UnusedNestedLookingFallbackEndsAtFirstClosingBrace) {
  // The inner closing brace ends the reference, leaving the outer brace as literal text.
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE:-${UNDEFINED_KEY}}", true), "value}");
}

TEST_F(EnvironmentSubstitution, FallbacksAreLiteralAndEndAtTheFirstClosingBrace) {
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:- https://host:123/a?b=c # '\t~ }"),
            " https://host:123/a?b=c # '\t~ ");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:-a:-b}"), "a:-b");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:-a}b}"), "ab}");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:-${DO_NOT_REPLACE_ME}}", true), "${DO_NOT_REPLACE_ME}");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:-$DO_NOT_REPLACE_ME}"), "$DO_NOT_REPLACE_ME");
}

TEST_F(EnvironmentSubstitution, EnvironmentContentsAreNeverReinterpreted) {
  EXPECT_EQ(substituteEnvironmentVariables("${REPLACE_ME}", true), "${DO_NOT_REPLACE_ME}");
  EXPECT_EQ(substituteEnvironmentVariables("${VALUE_WITH_ESCAPE}", true), "value$$");
  EXPECT_EQ(substituteEnvironmentVariables("${QUOTED_VALUE}"), "a\"b\\c\n\t");
  EXPECT_EQ(substituteEnvironmentVariables("${INVALID_MAP_VALUE}"), "value\nkey:value");
  ASSERT_EQ(::setenv("STRING_VALUE", "${1INVALID}$$${UNDEFINED_KEY}", 1), 0);
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE}", true), "${1INVALID}$$${UNDEFINED_KEY}");
}

TEST_F(EnvironmentSubstitution, ReplacementBoundariesDoNotCreateNewReferences) {
  ASSERT_EQ(::setenv("STRING_VALUE", "$", 1), 0);
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE}{UNDEFINED_KEY}", true), "${UNDEFINED_KEY}");
}

TEST_F(EnvironmentSubstitution, BackslashesDoNotEscapeReferences) {
  EXPECT_EQ(substituteEnvironmentVariables(R"(\${STRING_VALUE})", true), R"(\value)");
  EXPECT_EQ(substituteEnvironmentVariables(R"(\${env:STRING_VALUE})", true), R"(\value)");
}

TEST_F(EnvironmentSubstitution, EscapingTakesPrecedenceOverSubstitution) {
  EXPECT_EQ(substituteEnvironmentVariables("$${UNDEFINED_KEY}", true), "${UNDEFINED_KEY}");
  EXPECT_EQ(substituteEnvironmentVariables("$${1INVALID}", true), "${1INVALID}");
  EXPECT_EQ(substituteEnvironmentVariables("$$${STRING_VALUE}$$$$$"), "$value$$$");
  EXPECT_EQ(substituteEnvironmentVariables("$${STRING_VALUE} ${INT_VALUE} $${FLOAT_VALUE}"),
            "${STRING_VALUE} 1 ${FLOAT_VALUE}");
  EXPECT_EQ(substituteEnvironmentVariables("${UNDEFINED_KEY:-$${UNDEFINED_KEY}}", true),
            "${UNDEFINED_KEY:-${UNDEFINED_KEY}}");
  EXPECT_EQ(substituteEnvironmentVariables("${STRING_VALUE:-a$$b}"), "${STRING_VALUE:-a$b}");
}

TEST_F(EnvironmentSubstitution, RejectsInvalidReferencesAndUnsupportedPrefixes) {
  for (const auto* input : {"${1API_KEY}",
                            "${API_$KEY}",
                            "${STRING_VALUE:?error}",
                            "${STRING_VALUE-fallback}",
                            "${STRING_VALUE:+alternate}",
                            "${ STRING_VALUE}",
                            "${STRING_VALUE }",
                            "${ }",
                            "${env:}",
                            "${env:1API_KEY}",
                            "${env:STRING_VALUE:?error}",
                            "${sys:STRING_VALUE}",
                            "${ENV:STRING_VALUE}",
                            "${env:env:STRING_VALUE}"}) {
    SCOPED_TRACE(input);
    EXPECT_THROW(substituteEnvironmentVariables(input), cta::exception::UserError);
    EXPECT_THROW(substituteEnvironmentVariables(input, true), cta::exception::UserError);
  }
}

TEST_F(EnvironmentSubstitution, PreservesTextOutsideTheReferenceGrammar) {
  for (const std::string input :
       {"${", "${STRING_VALUE", "${}", "${UNDEFINED_KEY:-a\nb}", "${UNDEFINED_KEY:-a\rb}", "${UNDEFINED_KEY:-é}"}) {
    SCOPED_TRACE(input);
    EXPECT_EQ(substituteEnvironmentVariables(input, true), input);
  }
  EXPECT_EQ(substituteEnvironmentVariables("${}${STRING_VALUE}"), "${}value");
  EXPECT_EQ(substituteEnvironmentVariables("${broken\n${STRING_VALUE}}"), "${broken\nvalue}");
}

TEST_F(EnvironmentSubstitution, FailureDoesNotReturnPartialOutputOrLeakValues) {
  std::string result = "unchanged";
  EXPECT_THROW(result = substituteEnvironmentVariables("${STRING_VALUE} ${UNDEFINED_KEY}", true),
               cta::exception::UserError);
  EXPECT_EQ(result, "unchanged");
  try {
    result = substituteEnvironmentVariables("$$ ${STRING_VALUE} ${env:1INVALID:-secret}");
    FAIL() << "Expected invalid substitution error";
  } catch (const cta::exception::UserError& e) {
    EXPECT_NE(e.getMessageValue().find("byte 19"), std::string::npos);
    EXPECT_EQ(e.getMessageValue().find("secret"), std::string::npos);
    EXPECT_EQ(e.getMessageValue().find("value"), std::string::npos);
  }
  EXPECT_EQ(result, "unchanged");
}

TEST_F(EnvironmentSubstitution, HonorsStringViewBoundsAndEmbeddedNullsInScalarText) {
  const std::string input = "x${STRING_VALUE}y";
  EXPECT_EQ(substituteEnvironmentVariables(std::string_view(input).substr(1, input.size() - 2)), "value");
  const std::string withNull("a\0${STRING_VALUE}", 17);
  EXPECT_EQ(substituteEnvironmentVariables(withNull), std::string("a\0value", 7));
}

TEST_F(EnvironmentSubstitution, HandlesManyReferencesAndEscapes) {
  std::string input;
  std::string expected;
  for (int i = 0; i < 10000; ++i) {
    input += "${STRING_VALUE}$$";
    expected += "value$";
  }
  EXPECT_EQ(substituteEnvironmentVariables(input), expected);
}

}  // namespace
}  // namespace unitTests
