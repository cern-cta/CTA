/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {

/** A helper class allowing manipulation of arrays of ValueCountPairs, used as containers for running
 * counters for properties with multiple possible values. */
class ValueCountMap {
public:
  ValueCountMap (google::protobuf::RepeatedPtrField<serializers::ValueCountPair>* valueCountMap);
  void incCount(uint64_t value);
  void decCount(uint64_t value);
  void clear();
  uint64_t total();
  uint64_t minValue();
  uint64_t maxValue();
private:
  google::protobuf::RepeatedPtrField<serializers::ValueCountPair>& m_valueCountMap;
};

}} // namespace cta::objectstore