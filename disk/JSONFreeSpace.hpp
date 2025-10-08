/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/json/object/JSONCObject.hpp"

namespace cta::disk {

/**
 * This class allows  JSON-represent a FreeSpace object that is only a uint64_t value
 * {"freeSpace",42}
 */
class JSONFreeSpace: public cta::utils::json::object::JSONCObject {
public:
  JSONFreeSpace();
  ~JSONFreeSpace() final = default;
  void buildFromJSON(const std::string & json) override;
  std::string getJSON() override;
  std::string getExpectedJSONToBuildObject() const override;

  uint64_t m_freeSpace = 0;
};

} // namespace cta::disk
