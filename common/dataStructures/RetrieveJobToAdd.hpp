/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/MountPolicy.hpp"

#include <optional>
#include <string>

namespace cta::common::dataStructures {
struct RetrieveJobToAdd {
  RetrieveJobToAdd() = default;

  RetrieveJobToAdd(uint32_t cNb,
                   uint64_t fS,
                   std::string rra,
                   uint64_t filesize,
                   MountPolicy p,
                   time_t st,
                   std::optional<std::string> a,
                   std::optional<std::string> dsn)
      : copyNb(cNb),
        fSeq(fS),
        retrieveRequestAddress(rra),
        fileSize(filesize),
        policy(p),
        startTime(st),
        activity(a),
        diskSystemName(dsn) {}

  bool operator==(const RetrieveJobToAdd& rhs) const;
  bool operator!=(const RetrieveJobToAdd& rhs) const;

  uint32_t copyNb;
  uint64_t fSeq;
  std::string retrieveRequestAddress;
  uint64_t fileSize;
  MountPolicy policy;
  time_t startTime;
  std::optional<std::string> activity;
  std::optional<std::string> diskSystemName;
};

std::ostream& operator<<(std::ostream& os, const RetrieveJobToAdd& obj);

}  // namespace cta::common::dataStructures
