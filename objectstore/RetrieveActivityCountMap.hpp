/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "objectstore/cta.pb.h"

#include <list>

namespace cta::objectstore {

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
  explicit RetrieveActivityCountMap(
    google::protobuf::RepeatedPtrField<serializers::RetrieveActivityCountPair>* retrieveActivityCountMap);
  void incCount(const std::string& activity);
  void decCount(const std::string& activity);
  void clear();
  std::list<RetrieveActivityDescription> getActivities();

private:
  google::protobuf::RepeatedPtrField<serializers::RetrieveActivityCountPair>& m_activityCountMap;
};

std::string toString(const RetrieveActivityDescription&);
bool operator==(const serializers::RetrieveActivityCountPair&, const std::string&);

}  // namespace cta::objectstore
