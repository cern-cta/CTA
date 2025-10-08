/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/RequesterIdentity.hpp"

namespace cta::common::dataStructures {

/**
 * This request tells CTA to list all storage classes
 */
struct ListStorageClassRequest {
  ListStorageClassRequest() = default;
  bool operator==(const ListStorageClassRequest &rhs) const;
  bool operator!=(const ListStorageClassRequest &rhs) const;

  RequesterIdentity requester;
};

std::ostream &operator<<(std::ostream &os, const ListStorageClassRequest &obj);

} // namespace cta::common::dataStructures
