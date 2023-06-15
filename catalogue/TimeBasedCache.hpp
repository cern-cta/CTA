/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "catalogue/ValueAndTimeBasedCacheInfo.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"

#include <atomic>
#include <map>
#include <time.h>

namespace cta {
namespace catalogue {

template<typename Key, typename Value>
class TimeBasedCache {
public:
  /**
   * Constructor.
   *
   * @param m Maximum age of a cached value in seconds.
   */
  TimeBasedCache(const time_t m) : m_maxAgeSecs(m) {}

  /**
   * Get the cached value corresponding to the specified key.
   *
   * This method updates the cache when necessary.
   */
  template<typename Callable>
  ValueAndTimeBasedCacheInfo<Value> getCachedValue(const Key& key, const Callable& getNonCachedValue) {
    const time_t now = time(nullptr);

    threading::MutexLocker cacheLock(m_mutex);
    const auto cacheItor = m_cache.find(key);
    const bool cacheHit = m_cache.end() != cacheItor;

    if (cacheHit) {
      auto& cachedValue = *(cacheItor->second);
      const time_t ageSecs = now - cachedValue.timestamp;

      if (m_maxAgeSecs >= ageSecs) {  // Cached value is fresh
        return ValueAndTimeBasedCacheInfo<Value>(cachedValue.value, "Fresh value found in cache");
      }
      else {  // Cached value is stale
        cachedValue.value = getNonCachedValue();
        cachedValue.timestamp = ::time(nullptr);

        return ValueAndTimeBasedCacheInfo<Value>(cachedValue.value, "Stale value found and replaced in cache");
      }
    }
    else {  // No cache hit
      const auto emplaceResult =
        m_cache.emplace(std::make_pair(key, std::make_unique<TimestampedValue>(now, getNonCachedValue())));
      return ValueAndTimeBasedCacheInfo<Value>(emplaceResult.first->second->value,
                                               "First time value entered into cache");
    }
  }

  /**
   * Invalidates the cache.  This method should be called when it is known that
   * the values being cached have probably been changed.  For example an
   * operator has just modfied the mount policies and this is what is being
   * cached.
   */
  void invalidate() {
    threading::MutexLocker cacheLock(m_mutex);
    for (auto& cacheMaplet : m_cache) {
      auto& cachedValue = *(cacheMaplet.second);
      cachedValue.timestamp = 0;
    }
  }

private:
  /**
   * Maximum age of a cached value in seconds.
   */
  time_t m_maxAgeSecs;

  /**
   * Mutex to protect the cache.
   */
  threading::Mutex m_mutex;

  /**
   * A timestamped value.
   */
  struct TimestampedValue {
    /**
     * Constructor.
     */
    TimestampedValue() : timestamp(0) {}

    /**
     * Constructor.
     *
     * @param t The timestamp.
     * @param v The value.
     */
    TimestampedValue(const time_t t, const Value& v) : timestamp(t), value(v) {}

    /**
     * The timestamp.
     */
    time_t timestamp;

    /**
     * The value.
     */
    Value value;
  };  // struct TimestampedValue

  /**
   * The cache.
   */
  std::map<Key, std::unique_ptr<TimestampedValue>> m_cache;
};  // class TimeBasedCache

}  // namespace catalogue
}  // namespace cta
