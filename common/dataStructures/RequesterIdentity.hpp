/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct holds the username and group name of a given user
 */
struct RequesterIdentity {
  RequesterIdentity() = default;

  RequesterIdentity(const std::string& name, const std::string& group);

  bool operator==(const RequesterIdentity& rhs) const;

  bool operator!=(const RequesterIdentity& rhs) const;

  std::string name;
  std::string group;

};  // struct RequesterIdentity

std::ostream& operator<<(std::ostream& os, const RequesterIdentity& obj);

}  // namespace cta::common::dataStructures
