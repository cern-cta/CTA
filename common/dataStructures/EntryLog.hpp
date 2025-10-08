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
 * This struct is used across almost all administrative metadata, specifying
 * who, when, where created and modified a given metadata object
 */
struct EntryLog {
  EntryLog();
  EntryLog(const EntryLog& other) = default;
  EntryLog(const std::string& username, const std::string& host, const time_t time);

  EntryLog& operator=(const EntryLog& other) = default;
  bool operator==(const EntryLog& rhs) const;
  bool operator!=(const EntryLog& rhs) const;

  std::string username;
  std::string host;
  time_t time;
};

std::ostream& operator<<(std::ostream& os, const EntryLog& obj);

} // namespace cta::common::dataStructures
