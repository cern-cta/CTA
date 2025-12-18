/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <map>
#include <string>

namespace cta {

struct DiskSpaceReservationRequest : public std::map<std::string, uint64_t> {
  void addRequest(const std::string& diskSystemName, uint64_t size);
};

}  // namespace cta
