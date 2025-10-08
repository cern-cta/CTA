/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/json/object/JSONCObject.hpp"
#include "TestObject.hpp"

using namespace cta::utils::json;

namespace unitTests {

/**
 * This class is only use to unit test the JSONCObject class
 */
class JSONCTestObject : public object::JSONCObject, public TestObject {
public:
  JSONCTestObject();
  void buildFromJSON(const std::string & json) override;
  std::string getExpectedJSONToBuildObject() const override;
  std::string getJSON() override;
  virtual ~JSONCTestObject() = default;
};

}
