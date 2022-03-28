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

#include "RetrieveActivityCountMap.hpp"
#include "common/exception/Exception.hpp"

#include <algorithm>
#include <sstream>
#include <google/protobuf/util/json_util.h>

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RetrieveActivityCountMap::RetrieveActivityCountMap(
  google::protobuf::RepeatedPtrField<serializers::RetrieveActivityCountPair>* retrieveActivityCountMap):
  m_activityCountMap(*retrieveActivityCountMap) { }

//------------------------------------------------------------------------------
// RetrieveActivityCountMap::incCount()
//------------------------------------------------------------------------------
void RetrieveActivityCountMap::incCount(const std::string& activity) {
  // Find the entry for this value (might fail)
  auto counter = std::find_if(m_activityCountMap.begin(), m_activityCountMap.end(), 
    [&activity](serializers::RetrieveActivityCountPair pair) {return pair.activity() == activity;});
  if (counter != m_activityCountMap.end()) {
    if (counter->count() < 1) {
      std::stringstream err;
      err << "In ValueCountMap::incCount: unexpected count value=" << activity 
          << " count=" << counter->count();
      throw  cta::exception::Exception(err.str());
    } else {
      counter->set_count(counter->count()+1);
    }
  } else {
    // Create the new entry if necessary.
    auto newCounter = m_activityCountMap.Add();
    newCounter->set_activity(activity);
    newCounter->set_count(1);
  }
}

//------------------------------------------------------------------------------
// RetrieveActivityCountMap::decCount()
//------------------------------------------------------------------------------
void RetrieveActivityCountMap::decCount(const std::string& activity) {
  // Find the entry for this value. Failing is an error.
  auto counter = std::find_if(m_activityCountMap.begin(), m_activityCountMap.end(), 
    [&activity](serializers::RetrieveActivityCountPair pair) {return pair.activity() == activity;});
  if (counter == m_activityCountMap.end()) {
    std::stringstream err;
    err << "In RetrieveActivityCountMap::decCount: no entry found for value=" << activity;
    throw  cta::exception::Exception(err.str());
  }
  // Decrement the value and remove the entry if needed.
  if (counter->count() < 1) {
    std::stringstream err;
    err << "In ValueCountMap::decCount: entry with wrong count value=" << activity << " count=" << counter->count();
    throw  cta::exception::Exception(err.str());
  }
  counter->set_count(counter->count()-1);
  if (!counter->count()) {
    auto size=m_activityCountMap.size();
    counter->Swap(&(*(m_activityCountMap.end()-1)));
    m_activityCountMap.RemoveLast();
    // Cross check that the size has decreased.
    if (size -1 != m_activityCountMap.size()) {
      std::stringstream err;
      err << "In ValueCountMap::decCount: unexpected size after trimming empty entry. expectedSize=" << size -1 << " newSize=" << m_activityCountMap.size();
      throw  cta::exception::Exception(err.str());
    }
    // Cross check we cannot find the value.
    auto counter2 = std::find_if(m_activityCountMap.begin(), m_activityCountMap.end(), 
      [&activity](serializers::RetrieveActivityCountPair pair) {return pair.activity() == activity;});
  if (m_activityCountMap.end() != counter2) {
      std::stringstream err;
      err << "In ValueCountMap::decCount: still found the value after trimming empty entry. value=" << activity << " count=" << counter2->count();
      throw  cta::exception::Exception(err.str());
    }
  }
}

//------------------------------------------------------------------------------
// RetrieveActivityCountMap::getActivities()
//------------------------------------------------------------------------------
std::list<RetrieveActivityDescription> RetrieveActivityCountMap::getActivities() {
  std::list<RetrieveActivityDescription> ret;
  for (auto & ad: m_activityCountMap) {
    ret.push_back({ad.activity(), ad.count()});
  }
  return ret;
}


//------------------------------------------------------------------------------
// RetrieveActivityCountMap::clear()
//------------------------------------------------------------------------------
void RetrieveActivityCountMap::clear() {
  m_activityCountMap.Clear();
}

//------------------------------------------------------------------------------
// operator==()
//------------------------------------------------------------------------------
bool operator==(const serializers::RetrieveActivityCountPair & serialized, const std::string & memory) {
  return serialized.activity() == memory;
}

//------------------------------------------------------------------------------
// toString()
//------------------------------------------------------------------------------
std::string toString(const RetrieveActivityDescription & ad) {

  return ""; //TODO
}

}} // namespace cta::objectstore.