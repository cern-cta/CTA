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

#include "RAOParams.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

namespace castor::tape::tapeserver::rao {

const std::map<std::string,RAOParams::RAOAlgorithmType> RAOParams::c_raoAlgoStringTypeMap = {
  { "linear", RAOParams::RAOAlgorithmType::linear },
  { "random", RAOParams::RAOAlgorithmType::random },
  { "sltf",   RAOParams::RAOAlgorithmType::sltf }
};  

RAOParams::RAOAlgorithmType RAOParams::getAlgorithmType() const {
  try {
    return c_raoAlgoStringTypeMap.at(m_raoAlgorithmName);  
  } catch (const std::out_of_range&) {
    throw cta::exception::Exception("The algorithm name provided by the RAO configuration does not match any RAO algorithm type.");
  }
}

std::string RAOParams::getCTARAOAlgorithmNameAvailable() const {
  std::string ret;
  for(auto& kv : c_raoAlgoStringTypeMap) {
    ret += kv.first + " ";
  }
  if(ret.size()) {
    //remove last space
    ret.resize(ret.size()-1);
  }
  return ret;
}

std::string RAOParams::getMountedVid() const {
  if(m_vid.empty()) {
    throw cta::exception::Exception("In RAOData::getMountedVid(), no mounted vid found.");
  }
  return m_vid;
}

} // namespace castor::tape::tapeserver::rao
