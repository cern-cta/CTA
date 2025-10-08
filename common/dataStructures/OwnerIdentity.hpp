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
struct OwnerIdentity {

  OwnerIdentity();

  OwnerIdentity(uint32_t uid, uint32_t gid);

  bool operator==(const OwnerIdentity &rhs) const;

  bool operator!=(const OwnerIdentity &rhs) const;

  uint32_t uid;
  uint32_t gid;

}; // struct OwnerIdentity

std::ostream &operator<<(std::ostream &os, const OwnerIdentity &obj);

} // namespace cta::common::dataStructures
