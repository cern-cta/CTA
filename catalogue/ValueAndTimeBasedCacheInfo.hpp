/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <time.h>

namespace cta::catalogue {

template<typename Value>
struct ValueAndTimeBasedCacheInfo {
  Value value;
  std::string cacheInfo;

  ValueAndTimeBasedCacheInfo(const Value& v, const std::string& cInfo) : value(v), cacheInfo(cInfo) {}
};  // class ValueAndTimeBasedCacheInfo

}  // namespace cta::catalogue
