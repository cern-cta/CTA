/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "Backend.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class BackendAbstractTest : public ::testing::TestWithParam<cta::objectstore::Backend*> {
protected:
  BackendAbstractTest() {}

  virtual void SetUp() { m_os = GetParam(); }

  cta::objectstore::Backend* m_os;
};

}  // namespace unitTests
