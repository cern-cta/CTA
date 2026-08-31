/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ValidationResult.hpp"

#include <gtest/gtest.h>
#include <utility>

namespace unitTests {

TEST(ValidationResult, DefaultsToSuccess) {
  EXPECT_TRUE(cta::runtime::ValidationResult {}.ok());
}

TEST(ValidationResult, MergesPrefixesAndSortsErrors) {
  cta::runtime::ValidationResult child;
  child.addError("level", "is invalid");
  child.addError("format", "is invalid");

  cta::runtime::ValidationResult result;
  result.merge("logging", std::move(child));

  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.what(),
            "1) Field 'logging.format' is invalid.\n"
            "2) Field 'logging.level' is invalid.\n");
}

}  // namespace unitTests
