/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <string>

namespace cta::catalogue {

struct CreateMountPolicyAttributes {
  std::string name;
  uint64_t archivePriority;
  uint64_t minArchiveRequestAge;
  uint64_t retrievePriority;
  uint64_t minRetrieveRequestAge;
  std::string comment;
};

} // namespace cta::catalogue
