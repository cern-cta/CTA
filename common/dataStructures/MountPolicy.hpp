/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <stdint.h>
#include <list>
#include <map>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * Specifies the minimum criteria needed to warrant a mount
 */
struct MountPolicy {
  MountPolicy() = default;

  MountPolicy(const MountPolicy& other) = default;
  MountPolicy& operator=(const MountPolicy& other) = default;

  bool operator==(const MountPolicy &rhs) const;
  bool operator!=(const MountPolicy &rhs) const;

  std::string name;
  uint64_t archivePriority = 0;
  uint64_t archiveMinRequestAge = 0;
  uint64_t retrievePriority = 0;
  uint64_t retrieveMinRequestAge = 0;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;

  // As repack request does not have mount policy yet, we need to create a fake one to be able
  // to do a Retrieve mount or Archive mount
  static const MountPolicy s_defaultMountPolicyForRepack;

  MountPolicy(std::string_view mpName, uint64_t archivePriority, uint64_t archiveMinRequestAge,
    uint64_t retrievePriority, uint64_t retrieveMinRequestAge);
};

std::ostream& operator<<(std::ostream& os, const MountPolicy& obj);

} // namespace cta::common::dataStructures
