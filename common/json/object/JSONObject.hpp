/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace cta::utils::json::object {

/**
 * Interface that allows the objects that inherits from the implementations of this interface
 * to be built from JSON or to generate the json string representation from its attributes
 */
class JSONObject {
public:
  /**
   * Return the JSON representation of this object
   */
  virtual std::string getJSON() = 0;
  /**
   * Set the inherited object attributes from the json passed in parameter
   * @param json the json string used to set the inherited object attributes
   * @throws JSONObjectException if the json does not contain the correct key-value attributes
   */
  virtual void buildFromJSON(const std::string& json) = 0;
  /**
   * Return the inherited object expected JSON structure allowing to set its attributes
   * via the buildFromJSON() method
   * @return an example of JSON allowing to build the object e.g {"freeSpace",42}
   */
  virtual std::string getExpectedJSONToBuildObject() const = 0;
  virtual ~JSONObject() = default;
};

} // namespace cta::utils::json::object
