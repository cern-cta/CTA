
/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Helpers.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class ObjectStore : public ::testing::Test {
protected:
  void SetUp() override;
};

}  // namespace unitTests
