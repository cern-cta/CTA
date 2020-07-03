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

#include "common/make_unique.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"

#include <atomic>
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
   * When an instance of this class is destroyed, it will clear the
   * the specified std::atomic_flag.
   */
  class AtomicFlagClearer {
  public:
    /**
     * Constructor.
     *
     * @param flag The std::atomic_flag to be cleared.
     */
    AtomicFlagClearer(std::atomic_flag &flag): m_flag(flag) {
    }

    /**
     * Destructor that clears the std::atomic_flag.
     */
    ~AtomicFlagClearer() {
      m_flag.clear();
    }

  private:
    /**
     * The std::atomic_flag to be cleared.
     */
    std::atomic_flag &m_flag;
  };

  /**
   * Get the cached value corresponing to the specified key.
   *
   * This method updates the cache when necessary.
   */
  template<typename Callable> Value getCachedValue(const Key &key, const Callable &getNonCachedValue) {
    const time_t now = time(nullptr);

    threading::MutexLocker cacheLock(m_mutex);
    const auto cacheItor = m_cache.find(key);
    const bool cacheHit = m_cache.end() != cacheItor;

    if(cacheHit) {
      auto &cachedValue = *(cacheItor->second);
      threading::MutexLocker cachedValueLock(cachedValue.mutex);
      const time_t ageSecs = now - cachedValue.timestamp;

      if (m_maxAgeSecs >= ageSecs) { // Cached value is fresh
        return cachedValue.value;
      } else { // Cached value is stale
        cachedValue.value = getNonCachedValue();
        cachedValue.timestamp = ::time(nullptr);
        return cachedValue.value;
      }
    } else { // No cache hit
      const auto emplaceResult = m_cache.emplace(std::make_pair(key,
        cta::make_unique<TimestampedValue>(now, getNonCachedValue())));
      return emplaceResult.first->second->value;
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
    for(auto &cacheMaplet: m_cache) {
      auto &cachedValue = *(cacheMaplet.second);
      threading::MutexLocker cachedValueLock(cachedValue.mutex);
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
    TimestampedValue(): timestamp(0) {
    }

    /**
     * Constructor.
     *
     * @param t The timestamp.
     * @param v The value.
     */
    TimestampedValue(const time_t t, const Value &v): timestamp(t), value(v) {
    }

    /**
     * Mutex used to protect the timestamp and value.
     */
    threading::Mutex mutex;

    /**
     * The timestamp.
     */
    time_t timestamp;

    /**
     * The value.
     */
    Value value;
  }; // struct TimestampedValue

  /**
   * The cache.
   */
  std::map<Key, std::unique_ptr<TimestampedValue> > m_cache;
}; // class TimeBasedCache

} // namespace catalogue
} // namespace cta
