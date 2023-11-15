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

#include "RAOOptions.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"

namespace castor::tape::tapeserver::rao {

std::map<std::string,RAOOptions::CostHeuristicType> RAOOptions::c_mapStringCostHeuristicType = {
  {"cta",RAOOptions::CostHeuristicType::cta},
};

RAOOptions::RAOOptions(){}  
  
RAOOptions::RAOOptions(const std::string & options): m_options(options){
  cta::utils::splitString(m_options,',',m_allOptions);
}

RAOOptions::~RAOOptions() {
}

bool RAOOptions::getBooleanValue(const std::string& name) const {
  bool ret = false;
  bool found = false;
  auto itor = m_allOptions.begin();
  while(itor != m_allOptions.end() && !found){
    std::vector<std::string> keyValue;
    cta::utils::splitString(*itor,':',keyValue);
    if(keyValue.at(0) == name){
      try {
        std::string value = keyValue.at(1);
        cta::utils::toLower(value);
        if(value == "true"){
          ret = true;
        }//Any other value than "true" will be considered false
        found = true;
      } catch (const std::out_of_range & ex){
        std::string errorMsg = "The RAO configuration option parameter named " + name + " does not contain any boolean value";
        throw cta::exception::Exception(errorMsg);
      }
    }
  }
  if(!found){
    std::string errorMsg = "The RAO Configuration options (" + m_options + ") do not contain the key " + name;
    throw cta::exception::Exception(errorMsg);
  }
  return ret;
}

std::string RAOOptions::getStringValue(const std::string& name) const {
  std::string ret;
  bool found = false;
  auto itor = m_allOptions.begin();
  while(itor != m_allOptions.end() && !found){
    std::vector<std::string> keyValue;
    cta::utils::splitString(*itor,':',keyValue);
    if(keyValue.at(0) == name) {
      try {
        ret = keyValue.at(1);
        found = true;
      } catch (const std::out_of_range & ex){
        std::string errorMsg = "The RAO configuration option parameter named " + name + " does not contain any string value";
        throw cta::exception::Exception(errorMsg);
      }
    }
  }
  if(!found){
    std::string errorMsg = "The RAO Configuration options (" + m_options + ") do not contain the key " + name;
    throw cta::exception::Exception(errorMsg);
  }
  return ret;
}

RAOOptions::CostHeuristicType RAOOptions::getCostHeuristicType() {
  try {
    std::string costHeuristicName = getStringValue("cost_heuristic_name");
    return c_mapStringCostHeuristicType.at(costHeuristicName);
  } catch(const cta::exception::Exception&) {
    throw;
  } catch(const std::out_of_range&) {
    throw cta::exception::Exception("In RAOOptions::getCostHeuristic(), unable to find the cost heuristic to use from the RAOAlgorithmOptions configuration line (" + m_options + ")");
  }
}

RAOOptions::FilePositionEstimatorType RAOOptions::getFilePositionEstimatorType() {
  //For now we only support interpolation.
  //This method should be modified if we want to support other types of FilePositionEstimator
  return RAOOptions::FilePositionEstimatorType::interpolation;
}

std::string RAOOptions::getOptionsString() {
  return m_options;
}


} // namespace castor::tape::tapeserver::rao
