/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DiskSystem.hpp"
#include "common/json/object/JSONCObject.hpp"

namespace cta::disk {

/**
 * This class allows to transform a DiskSystem object into a JSON string
 * and to build a DiskSystem object from a JSON string
 */
class JSONDiskSystem : public DiskSystem, public cta::utils::json::object::JSONCObject {
public:
  JSONDiskSystem();
  explicit JSONDiskSystem(const DiskSystem& diskSystem);
  /**
   * Builds the DiskSystem object with the json passed in parameter
   */
  void buildFromJSON(const std::string& json) override;
  /**
   * Get the json string representation of the inherited DiskSystem object
   */
  std::string getJSON() override;
  ~JSONDiskSystem() final = default;
};

}  // namespace cta::disk
