/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TomlParser.hpp"

#include <chrono>

namespace cta::runtime::parsing {
/**
 * Converts a toml::date_time to a std::chrono::system_clock::time_point.
 * Adjusts for timezone offset if present, returns result in UTC.
 *
 * @param dt the date in TOML format
 * @return the `time_point` value
 */
std::chrono::system_clock::time_point dateTimeToTimePoint(const toml::date_time& dt) {
  std::tm tm {};
  tm.tm_year = dt.date.year - 1900;
  tm.tm_mon = dt.date.month - 1;
  tm.tm_mday = dt.date.day;
  tm.tm_hour = dt.time.hour;
  tm.tm_min = dt.time.minute;
  tm.tm_sec = dt.time.second;
  tm.tm_isdst = 0;  // UTC has no DST

  std::time_t result = ::timegm(&tm);
  if (dt.offset.has_value()) {
    result -= dt.offset->minutes * 60;
  }
  return std::chrono::system_clock::from_time_t(result);
}

}  // namespace cta::runtime::parsing
