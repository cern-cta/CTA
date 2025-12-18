/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

// This is a simplified main for integration tests. There is not point in using
// valgrind with it, and we will not use gmock with it either (pure gtest).
int main(int argc, char** argv) {
  int newArgc = argc;
  ::testing::InitGoogleTest(&newArgc, argv);
  return RUN_ALL_TESTS();
}
