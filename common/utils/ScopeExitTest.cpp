/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/utils/ScopeExit.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace cta::utils {
namespace {

TEST(ScopeExitTest, RunsOnceOnNormalExitAndEarlyReturn) {
  for (const bool earlyReturn : {false, true}) {
    int calls = 0;
    const auto operation = [&] {
      const ScopeExit cleanup([&] { ++calls; });
      EXPECT_EQ(0, calls);
      if (earlyReturn) {
        return;
      }
      EXPECT_EQ(0, calls);
    };
    operation();
    EXPECT_EQ(1, calls);
  }
}

TEST(ScopeExitTest, UnwindsInReverseDeclarationOrderAndPreservesException) {
  std::vector<int> order;
  order.reserve(2);
  try {
    const ScopeExit first([&] { order.push_back(1); });
    const ScopeExit second([&] { order.push_back(2); });
    throw std::runtime_error("operation failed");
  } catch (const std::runtime_error& ex) {
    EXPECT_STREQ("operation failed", ex.what());
  }
  EXPECT_EQ((std::vector<int> {2, 1}), order);
}

TEST(ScopeExitTest, OwnsMoveOnlyCallbackWithoutTransferringGuardOwnership) {
  int result = 0;
  {
    const ScopeExit cleanup([value = std::make_unique<int>(42), &result] { result = *value; });
    using Guard = std::remove_cv_t<decltype(cleanup)>;
    static_assert(!std::is_copy_constructible_v<Guard>);
    static_assert(!std::is_move_constructible_v<Guard>);
    static_assert(!std::is_copy_assignable_v<Guard>);
    static_assert(!std::is_move_assignable_v<Guard>);
    EXPECT_EQ(0, result);
  }
  EXPECT_EQ(42, result);
}

}  // namespace
}  // namespace cta::utils
