
/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gtest/gtest.h>

#include "Helpers.hpp"

namespace unitTests {

  class ObjectStore : public ::testing::Test {
  protected:
    virtual void SetUp() override;
  };

}
