/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
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

#include "RAOConfigurationData.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
const std::map<std::string,RAOConfigurationData::RAOAlgorithmType> RAOConfigurationData::c_raoAlgoStringTypeMap = {
  {"linear",RAOConfigurationData::RAOAlgorithmType::linear},
  {"random",RAOConfigurationData::RAOAlgorithmType::random},
  {"sltf",RAOConfigurationData::RAOAlgorithmType::sltf}
};  

RAOConfigurationData::RAOConfigurationData():m_useRAO(false){}

RAOConfigurationData::RAOConfigurationData(const bool useRAO, const std::string& raoAlgorithmName, const std::string & raoAlgorithmOptions, const std::string & vid):m_useRAO(useRAO), m_raoAlgorithmName(raoAlgorithmName), 
    m_raoAlgorithmOptions(raoAlgorithmOptions), m_vid(vid) {

}

RAOConfigurationData::RAOConfigurationData(const RAOConfigurationData& other) {
  if(this != &other){
    m_useRAO = other.m_useRAO;
    m_raoAlgorithmName = other.m_raoAlgorithmName;
    m_raoAlgorithmOptions = other.m_raoAlgorithmOptions;
    m_vid = other.m_vid;
  }
}

RAOConfigurationData& RAOConfigurationData::operator=(const RAOConfigurationData& other) {
  if(this != &other){
    m_useRAO = other.m_useRAO;
    m_raoAlgorithmName = other.m_raoAlgorithmName;
    m_raoAlgorithmOptions = other.m_raoAlgorithmOptions;
    m_vid = other.m_vid;
  }
  return *this;
}

bool RAOConfigurationData::useRAO() const {
  return m_useRAO;
}

std::string RAOConfigurationData::getRAOAlgorithmName() const {
  return m_raoAlgorithmName;
}

RAOOptions RAOConfigurationData::getRAOAlgorithmOptions() const {
  return m_raoAlgorithmOptions;
}

void RAOConfigurationData::disableRAO(){
  m_useRAO = false;
}

RAOConfigurationData::RAOAlgorithmType RAOConfigurationData::getAlgorithmType() const {
  try {
    return c_raoAlgoStringTypeMap.at(m_raoAlgorithmName);  
  } catch (const std::out_of_range &){
    throw cta::exception::Exception("The algorithm name provided by the RAO configuration does not match any RAO algorithm type.");
  }
}

std::string RAOConfigurationData::getCTARAOAlgorithmNameAvailable() const {
  std::string ret;
  for(auto & kv: c_raoAlgoStringTypeMap){
    ret += kv.first + " ";
  }
  if(ret.size()){
    //remove last space
    ret.resize(ret.size()-1);
  }
  return ret;
}

std::string RAOConfigurationData::getMountedVid() const {
  if(m_vid.empty()){
    throw cta::exception::Exception("In RAOData::getMountedVid(), no mounted vid found.");
  }
  return m_vid;
}





}}}}