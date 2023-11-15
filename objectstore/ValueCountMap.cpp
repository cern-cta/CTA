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
      [&](decltype(*m_valueCountMap.begin()) pair) {  // cppcheck-suppress internalAstError
        if (ret < pair.value()) ret = pair.value();
      });
  return ret;
}

template<>
uint64_t ValueCountMap<google::protobuf::RepeatedPtrField<serializers::ValueCountPair>, uint64_t>::minValue() {
  if (!m_valueCountMap.size()) throw  cta::exception::Exception("In ValueCountMap::minValue: empty map");
  uint64_t ret = std::numeric_limits<uint64_t>::max();
  std::for_each(m_valueCountMap.begin(), m_valueCountMap.end(),
      [&](decltype(*m_valueCountMap.begin()) pair) {
        if (ret > pair.value()) ret = pair.value();
      });
  return ret;
}

} // namespace cta::objectstore
