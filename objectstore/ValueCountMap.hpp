/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <algorithm>
#include <map>
#include <string>
#include <utility>

#include "objectstore/cta.pb.h"

namespace cta::objectstore {

/** A helper class allowing manipulation of arrays of ValueCountPairs, used as containers for running
 * counters for properties with multiple possible values. */

template<typename Type, typename Key>
class ValueCountMap {
public:
  explicit ValueCountMap(Type* valueCountMap);
  void incCount(const Key& value);
  void decCount(const Key& value);
  void clear();
  uint64_t total();
  Key minValue();
  Key maxValue();
  /**
   * Returns the map with the Key and the NON-0 counters of the Key element
   */
  std::map<Key, uint64_t> getMap();

protected:
  // google::protobuf::RepeatedPtrField<serializers::ValueCountPair>& m_valueCountMap;
  Type& m_valueCountMap;
};

template<typename Type, typename Key>
void ValueCountMap<Type, Key>::decCount(const Key& value) {
  // Find the entry for this value. Failing is an error.
  auto counter = std::find_if(m_valueCountMap.begin(), m_valueCountMap.end(), [&value](const auto& pair) {
    return pair.value() == value;
  });
  if (counter == m_valueCountMap.end()) {
    std::stringstream err;
    err << "In ValueCountMap::decCount: no entry found for value=" << value;
    throw cta::exception::Exception(err.str());
  }
  // Decrement the value and remove the entry if needed.
  if (counter->count() < 1) {
    std::stringstream err;
    err << "In ValueCountMap::decCount: entry with wrong count value=" << value << " count=" << counter->count();
    throw cta::exception::Exception(err.str());
  }
  counter->set_count(counter->count() - 1);
  if (!counter->count()) {
    auto size = m_valueCountMap.size();
    counter->Swap(&(*(m_valueCountMap.end() - 1)));
    m_valueCountMap.RemoveLast();
    // Cross check that the size has decreased.
    if (size - 1 != m_valueCountMap.size()) {
      std::stringstream err;
      err << "In ValueCountMap::decCount: unexpected size after trimming empty entry. expectedSize=" << size - 1
          << " newSize=" << m_valueCountMap.size();
      throw cta::exception::Exception(err.str());
    }
    // Cross check we cannot find the value.
    auto counter2 = std::find_if(m_valueCountMap.begin(), m_valueCountMap.end(), [&value](const auto& pair) {
      return pair.value() == value;
    });
    if (m_valueCountMap.end() != counter2) {
      std::stringstream err;
      err << "In ValueCountMap::decCount: still found the value after trimming empty entry. value=" << counter2->value()
          << " count=" << counter2->count();
      throw cta::exception::Exception(err.str());
    }
  }
}

template<typename Type, typename Key>
void ValueCountMap<Type, Key>::incCount(const Key& value) {
  // Find the entry for this value (might fail)
  auto counter = std::find_if(m_valueCountMap.begin(), m_valueCountMap.end(), [&value](const auto& pair) {
    return pair.value() == value;
  });
  if (counter != m_valueCountMap.end()) {
    if (counter->count() < 1) {
      std::stringstream err;
      err << "In ValueCountMap::incCount: unexpected count value=" << counter->value() << " count=" << counter->count();
      throw cta::exception::Exception(err.str());
    } else {
      counter->set_count(counter->count() + 1);
    }
  } else {
    // Create the new entry if necessary.
    auto newCounter = m_valueCountMap.Add();
    newCounter->set_value(value);
    newCounter->set_count(1);
  }
}

template<typename Type, typename Key>
void ValueCountMap<Type, Key>::clear() {
  m_valueCountMap.Clear();
}

template<typename Type, typename Key>
std::map<Key, uint64_t> ValueCountMap<Type, Key>::getMap() {
  std::map<Key, uint64_t> ret;
  for (auto itor = m_valueCountMap.begin(); itor != m_valueCountMap.end(); ++itor) {
    if (itor->count() != 0) {
      std::pair<Key, uint64_t> pairToInsert;
      pairToInsert.first = itor->value();
      pairToInsert.second = itor->count();
      ret.insert(pairToInsert);
    }
  }
  return ret;
}

template<typename Type, typename Key>
uint64_t ValueCountMap<Type, Key>::total() {
  uint64_t ret = 0;
  std::for_each(m_valueCountMap.begin(), m_valueCountMap.end(), [&ret](const auto& pair) {
    if (pair.count() < 1) {
      std::stringstream err;
      err << "In ValueCountMap::total: unexpected count value=" << pair.value() << " count=" << pair.count();
      throw cta::exception::Exception(err.str());
    }
    ret += pair.count();
  });
  return ret;
}

using ValueCountMapUint64 = ValueCountMap<google::protobuf::RepeatedPtrField<serializers::ValueCountPair>, uint64_t>;
using ValueCountMapString =
  ValueCountMap<google::protobuf::RepeatedPtrField<serializers::StringCountPair>, std::string>;

}  // namespace cta::objectstore
