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
void RetrieveActivityCountMap::incCount(const RetrieveActivityDescription& activityDescription) {
  // Find the entry for this value (might fail)
  auto counter = std::find(m_activityCountMap.begin(), m_activityCountMap.end(), activityDescription);
  if (counter != m_activityCountMap.end()) {
    if (counter->count() < 1) {
      std::stringstream err;
      err << "In ValueCountMap::incCount: unexpected count value=" << toString(counter->retrieve_activity_weight()) 
          << " count=" << counter->count();
      throw  cta::exception::Exception(err.str());
    } else {
      counter->set_count(counter->count()+1);
      // Update the weight to the latest version (in case weights got updated since last time).
      if (counter->retrieve_activity_weight().creation_time() < activityDescription.creationTime) {
        counter->mutable_retrieve_activity_weight()->set_weight(activityDescription.weight);
        counter->mutable_retrieve_activity_weight()->set_creation_time(activityDescription.creationTime);
      }
    }
  } else {
    // Create the new entry if necessary.
    auto newCounter = m_activityCountMap.Add();
    newCounter->mutable_retrieve_activity_weight()->set_priority(activityDescription.priority);
    newCounter->mutable_retrieve_activity_weight()->set_disk_instance_name(activityDescription.diskInstanceName);
    newCounter->mutable_retrieve_activity_weight()->set_activity(activityDescription.activity);
    newCounter->mutable_retrieve_activity_weight()->set_weight(activityDescription.weight);
    newCounter->mutable_retrieve_activity_weight()->set_creation_time(activityDescription.creationTime);
    newCounter->set_count(1);
  }
}

//------------------------------------------------------------------------------
// RetrieveActivityCountMap::decCount()
//------------------------------------------------------------------------------
void RetrieveActivityCountMap::decCount(const RetrieveActivityDescription& activityDescription) {
  // Find the entry for this value. Failing is an error.
  auto counter = std::find(m_activityCountMap.begin(), m_activityCountMap.end(), activityDescription);
  if (counter == m_activityCountMap.end()) {
    std::stringstream err;
    err << "In RetrieveActivityCountMap::decCount: no entry found for value=" << toString(activityDescription);
    throw  cta::exception::Exception(err.str());
  }
  // Decrement the value and remove the entry if needed.
  if (counter->count() < 1) {
    std::stringstream err;
    err << "In ValueCountMap::decCount: entry with wrong count value=" << toString(activityDescription) << " count=" << counter->count();
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
    auto counter2 = std::find(m_activityCountMap.begin(), m_activityCountMap.end(), activityDescription);
    if (m_activityCountMap.end() != counter2) {
      std::stringstream err;
      err << "In ValueCountMap::decCount: still found the value after trimming empty entry. value=" << toString(counter2->retrieve_activity_weight()) << " count=" << counter2->count();
      throw  cta::exception::Exception(err.str());
    }
  }
}

//------------------------------------------------------------------------------
// RetrieveActivityCountMap::getActivities()
//------------------------------------------------------------------------------
std::list<RetrieveActivityDescription> RetrieveActivityCountMap::getActivities(uint64_t priority) {
  std::list<RetrieveActivityDescription> ret;
  for (auto & ad: m_activityCountMap) {
    if (ad.retrieve_activity_weight().priority() == priority)
      ret.push_back({ad.retrieve_activity_weight().priority(), ad.retrieve_activity_weight().disk_instance_name(), 
          ad.retrieve_activity_weight().activity(), ad.retrieve_activity_weight().creation_time(),
          ad.retrieve_activity_weight().weight(), ad.count()});
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
bool operator==(const serializers::RetrieveActivityCountPair & serialized, const RetrieveActivityDescription & memory) {
  return (serialized.retrieve_activity_weight().priority() == memory.priority)
      && (serialized.retrieve_activity_weight().disk_instance_name() == memory.diskInstanceName)
      && (serialized.retrieve_activity_weight().activity() == memory.activity);
}

//------------------------------------------------------------------------------
// toString()
//------------------------------------------------------------------------------
std::string toString(const RetrieveActivityDescription & ad) {
  serializers::RetrieveActivityWeight raw;
  raw.set_priority(ad.priority);
  raw.set_disk_instance_name(ad.diskInstanceName);
  raw.set_activity(ad.activity);
  raw.set_creation_time(ad.creationTime);
  raw.set_weight(ad.weight);
  return toString(raw);
}

//------------------------------------------------------------------------------
// toString()
//------------------------------------------------------------------------------
std::string toString(const serializers::RetrieveActivityWeight & raw){
  using namespace google::protobuf::util;

  std::string json;
  JsonPrintOptions options;

  options.always_print_primitive_fields = true;
  MessageToJsonString(raw, &json, options);

  return json;
}
    

}} // namespace cta::objectstore.