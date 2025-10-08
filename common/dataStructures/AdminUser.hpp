/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <stdint.h>
#include <string>

#include "common/dataStructures/EntryLog.hpp"

namespace cta::common::dataStructures {

/**
 * This is the administrative user which contains the username of the admin
 */
struct AdminUser {
  AdminUser() = default;
  bool operator==(const AdminUser &rhs) const;
  bool operator!=(const AdminUser &rhs) const;

  std::string name;
  EntryLog creationLog;
  EntryLog lastModificationLog;
  std::string comment;
};

std::ostream &operator<<(std::ostream &os, const AdminUser &obj);

} // namespace cta::common::dataStructures
