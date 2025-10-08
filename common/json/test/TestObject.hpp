/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace unitTests {

  /**
   * Object that will be used
   * to unit test the JSONCTestObject
   */
  struct TestObject {
    uint64_t integer_number = 0;
    std::string str;
    double double_number = 0.0;
  };

}
