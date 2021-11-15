/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <limits>
#include <string>

#include "ValueCountMap.hpp"

namespace cta {
namespace objectstore {

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
      [&](decltype(*m_valueCountMap.begin()) pair) {
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

}  // namespace objectstore
}  // namespace cta
