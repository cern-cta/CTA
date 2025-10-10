/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <limits>
#include <string>

#include "ValueCountMap.hpp"

namespace cta::objectstore {

template<>
ValueCountMap<google::protobuf::RepeatedPtrField<serializers::ValueCountPair>, uint64_t>::ValueCountMap(
  google::protobuf::RepeatedPtrField<serializers::ValueCountPair>* valueCountMap):
  m_valueCountMap(*valueCountMap) { }

template<>
ValueCountMap<google::protobuf::RepeatedPtrField<serializers::StringCountPair>, std::string>::ValueCountMap(
  google::protobuf::RepeatedPtrField<serializers::StringCountPair>* valueCountMap):
  m_valueCountMap(*valueCountMap) { }

template<>
uint64_t ValueCountMap<google::protobuf::RepeatedPtrField<serializers::ValueCountPair>, uint64_t>::maxValue() {
  if (!m_valueCountMap.size())
    throw  cta::exception::Exception("In ValueCountMap::maxValue: empty map");
  uint64_t ret = std::numeric_limits<uint64_t>::min();
  std::for_each(m_valueCountMap.begin(), m_valueCountMap.end(),
      // https://trac.cppcheck.net/ticket/10739
      [&ret](decltype(*m_valueCountMap.begin()) pair) { // cppcheck-suppress internalAstError
        if(ret < pair.value()) ret = pair.value();
      });
  return ret;
}

template<>
uint64_t ValueCountMap<google::protobuf::RepeatedPtrField<serializers::ValueCountPair>, uint64_t>::minValue() {
  if (!m_valueCountMap.size()) throw  cta::exception::Exception("In ValueCountMap::minValue: empty map");
  uint64_t ret = std::numeric_limits<uint64_t>::max();
  std::for_each(m_valueCountMap.begin(), m_valueCountMap.end(),
      [&ret](decltype(*m_valueCountMap.begin()) pair) {
        if(ret > pair.value()) ret = pair.value();
      });
  return ret;
}

} // namespace cta::objectstore
