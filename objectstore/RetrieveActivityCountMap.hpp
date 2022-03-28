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

#include "objectstore/cta.pb.h"

#include <list>

namespace cta { namespace objectstore {

struct RetrieveActivityDescription {
  std::string activity;
  uint64_t count;
};

/** A helper class allowing manipulation of arrays of ValueCountPairs, used as containers for running
 * counters for properties with multiple possible values. When considering the retrieve mounts, all activities
 * will be considered for the same mount (and highest priority one will be accounted). So this class does not
 * select any and gives the full list in getActivities(). Having multiple activities sharing the drive is not
 * expected to be a frequent occurrence. */
class RetrieveActivityCountMap {
public:
  RetrieveActivityCountMap (google::protobuf::RepeatedPtrField<serializers::RetrieveActivityCountPair>* retrieveActivityCountMap);
  void incCount(const std::string &activity);
  void decCount(const std::string &activity);
  void clear();
  std::list<RetrieveActivityDescription> getActivities();
private:
  google::protobuf::RepeatedPtrField<serializers::RetrieveActivityCountPair>& m_activityCountMap;
};

std::string toString(const RetrieveActivityDescription &);
bool operator==(const serializers::RetrieveActivityCountPair &, const std::string &);

}} // namespace cta::objectstore