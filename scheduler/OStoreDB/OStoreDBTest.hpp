/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "objectstore/Backend.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class OStoreAbstractTest : public ::testing::TestWithParam<cta::objectstore::Backend*> {
protected:
  OStoreAbstractTest() {}

  void SetUp() override { m_backend = GetParam(); }

  cta::objectstore::Backend* m_backend;
};

}  // namespace unitTests
