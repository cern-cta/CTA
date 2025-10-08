/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
