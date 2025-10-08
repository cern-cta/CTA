/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <map>
#include <optional>
#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

/**
 * This struct contains information about which drive was responsible for a
 * specific tape operation (read/write/label) and when did it happen
 */
struct TapeLog {

  TapeLog();

  bool operator==(const TapeLog &rhs) const;

  bool operator!=(const TapeLog &rhs) const;

  std::string drive;
  time_t time;

}; // struct TapeLog

std::ostream &operator<<(std::ostream &os, const TapeLog &obj);

std::ostream &operator<<(std::ostream &os, const std::optional<TapeLog> &obj);

} // namespace cta::common::dataStructures
