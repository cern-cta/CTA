/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"

#include <map>
#include <time.h>

namespace cta {
namespace catalogue {

template <typename Key, typename Value> class TimeBasedCache {
public:

  /**
   * Constructor.
   *
   * @param m Maximum age of a cached value in seconds.
   */
  TimeBasedCache(const time_t m): m_maxAgeSecs(m) {
  }

  /**
   * Get the cached value corresponing to the specified key.
   *
   * This method updates the cache when necessary.
   */
  template<typename Callable> Value getCachedValue(const Key &key, const Callable &getNonCachedValue) {
    const time_t now = time(nullptr);

    threading::MutexLocker lock(m_mutex);
    const auto cacheItor = m_cache.find(key);
    const bool cacheHit = m_cache.end() != cacheItor;

    if(cacheHit) {
      const auto &cachedValue = cacheItor->second;
      const time_t ageSecs = now - cachedValue.timestamp;
      if (m_maxAgeSecs >= ageSecs) {
        return cachedValue.value;
      } else {
        const auto &newValue = TimestampedValue(now, getNonCachedValue());
        m_cache[key] = newValue;
        return newValue.value;
      }
    } else { // No cache hit
      const auto &newValue = TimestampedValue(now, getNonCachedValue());
      m_cache[key] = newValue;
      return newValue.value;
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
   * A timestamped tape copy to tape pool map.
   */
  struct TimestampedValue {
    /**
     * The timestamp.
     */
    time_t timestamp;

    /**
     * The value.
     */
    Value value;

    /**
     * Constructor.
     */
    TimestampedValue(): timestamp(0) {
    }

    /**
     * Constructor.
     *
     * @param t The timestamp.
     * @param v The value.
     */
    TimestampedValue(const time_t t, const Value &v):
      timestamp(t),
      value(v) {
    }
  }; // struct TimestampedValue

  /**
   * The cache.
   */
  std::map<Key, TimestampedValue> m_cache;
}; // class TimeBasedCache

} // namespace catalogue
} // namespace cta
