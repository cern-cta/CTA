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

#include "RAOConfig.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
const std::map<std::string,RAOConfig::RAOAlgorithmType> RAOConfig::c_raoAlgoStringTypeMap = {
  {"linear",RAOConfig::RAOAlgorithmType::linear},
  {"random",RAOConfig::RAOAlgorithmType::random},
  {"sltf",RAOConfig::RAOAlgorithmType::sltf}
};  

RAOConfig::RAOConfig(){}

RAOConfig::RAOConfig(const bool useRAO, const std::string& raoAlgorithmName, const std::string raoAlgorithmOptions):m_useRAO(useRAO), m_raoAlgorithmName(raoAlgorithmName), m_raoAlgorithmOptions(raoAlgorithmOptions) {

}

bool RAOConfig::useRAO() const {
  return m_useRAO;
}

std::string RAOConfig::getRAOAlgorithmName() const {
  return m_raoAlgorithmName;
}

std::string RAOConfig::getRAOAlgorithmOptions() const {
  return m_raoAlgorithmOptions;
}

void RAOConfig::disableRAO(){
  m_useRAO = false;
}

RAOConfig::RAOAlgorithmType RAOConfig::getAlgorithmType() const {
  try {
    return c_raoAlgoStringTypeMap.at(m_raoAlgorithmName);  
  } catch (const std::out_of_range &){
    throw cta::exception::Exception("The algorithm name provided by the RAO configuration does not match any RAO algorithm type.");
  }
}

std::string RAOConfig::getCTARAOAlgorithmNameAvailable() const {
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



}}}}