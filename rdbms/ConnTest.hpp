/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/LoginFactory.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_ConnTest : public ::testing::TestWithParam<cta::rdbms::LoginFactory*> {
protected:
  /**
   * The login object used to create database connections.
   */
  cta::rdbms::Login m_login;

  virtual void SetUp();

  virtual void TearDown();

};  // cta_rdbms_ConnTest

}  // namespace unitTests
