/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskSpaceReservationRequest.hpp"

#include "common/exception/Exception.hpp"

namespace cta {

void DiskSpaceReservationRequest::addRequest(const std::string& diskSystemName, uint64_t size) {
  try {
    at(diskSystemName) += size;
  } catch (std::out_of_range&) {
    operator[](diskSystemName) = size;
  }
}

}  // namespace cta
