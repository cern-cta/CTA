/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "VirtualOrganization.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/EntryLog.hpp"

#include <list>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace cta::common::dataStructures {

/**
* This struct specifies the number of copies that a file tagged with it should
* have. it may also indicate the VO owning the file and what kind of data the
* file contains
*/
struct StorageClass {
  /**
* Constructor that sets all integer member-variables to zero and all string
* member-variables to the empty string.
*/
  StorageClass();

  /**
* Returns true if the specified StorageClass object is considered equal to
* this one.
*
* Please note that two StorageClass objects are considered equal if and only
* if their name member-variables match, i.e.
*
*    name==rhs.name;
*
* All other member-variables are intentionally ignored by this equality
* operator.
*
* @param rhs The other StorageClass object on the right-hand side of the
* equality operator.
* @return True if the specified StorageClass object is considered equal to
* this one.
*/
  bool operator==(const StorageClass& rhs) const;

  /**
* Returns the logical negation of operator==().
*
* @param rhs The other StorageClass object on the right-hand side of the
* inequality operator.
* @return The logical negation of operator==().
*/
  bool operator!=(const StorageClass& rhs) const;

  /**
* The name of the storage class.
*
* Please note that the name of a storage class is only gauranteed to be
* unique within its disk instance.
*/
  std::string name;

  /**
* The number of copies on tape.
*/
  uint64_t nbCopies = 0;

  /**
* The virtual organization to which this storage class belongs
*/
  VirtualOrganization vo;

  /**
* The creation log.
*/
  EntryLog creationLog;

  /**
* Th alst modification log.
*/
  EntryLog lastModificationLog;

  /**
* The comment.
*/
  std::string comment;

};  // struct StorageClass

std::ostream& operator<<(std::ostream& os, const StorageClass& obj);

/**
 * @class StorageClassCache
 * @brief Thread-safe in-memory cache of StorageClass objects used during archival
 * queueing operations to avoid hammering catalogue for every request slowing down the queueing
 *
 * Members:
 *  - std::unordered_map<std::string, StorageClass> m_cache: maps name → StorageClass.
 *  - mutable std::shared_mutex m_mutex: protects the cache.
 *  - Catalogue* m_catalogue: source of truth for StorageClass objects.
 *
 * Methods:
 *  - explicit StorageClassCache(Catalogue* cat):
 *      Constructor. Populates the cache by calling refresh().
 *
 *  - std::optional<StorageClass> find(const std::string& name) const:
 *      Lookup a StorageClass by name. Returns nullopt if not found.
 *      Thread-safe for concurrent readers.
 *
 *  - void refresh():
 *      Reloads all StorageClass objects from the Catalogue.
 *      Acquires exclusive lock; not const.
 */
class StorageClassCache {
public:
  explicit StorageClassCache(cta::catalogue::Catalogue* cat) : m_catalogue(cat) { refresh(); }

  std::optional<StorageClass> find(const std::string& name) const {
    std::shared_lock lock(m_mutex);
    auto it = m_cache.find(name);
    if (it == m_cache.end()) {
      lock.unlock();  // release shared lock before exclusive
      refresh();
      lock.lock();  // reacquire shared lock
      it = m_cache.find(name);
      if (it == m_cache.end()) {
        return std::nullopt;
      }
    }
    return it->second;
  }

  void refresh() const {
    auto all = m_catalogue->StorageClass()->getStorageClasses();
    std::unique_lock lock(m_mutex);
    m_cache.clear();
    for (const auto& sc : all) {
      m_cache.emplace(sc.name, sc);
    }
  }

private:
  mutable std::shared_mutex m_mutex;
  cta::catalogue::Catalogue* m_catalogue;
  mutable std::unordered_map<std::string, common::dataStructures::StorageClass> m_cache;
};

}  // namespace cta::common::dataStructures
