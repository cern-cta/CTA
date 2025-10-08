/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "NonConfigurableRAOAlgorithmFactory.hpp"
#include "LinearRAOAlgorithm.hpp"
#include "RandomRAOAlgorithm.hpp"

namespace castor::tape::tapeserver::rao {

std::unique_ptr<RAOAlgorithm> NonConfigurableRAOAlgorithmFactory::createRAOAlgorithm() {
  std::unique_ptr<RAOAlgorithm> ret;
  switch(m_type){
    case RAOParams::linear:{
      ret.reset(new LinearRAOAlgorithm());
      break;
    }
    case RAOParams::random:{
      ret.reset(new RandomRAOAlgorithm());
      break;
    }
    default:
    {
      throw cta::exception::Exception("In NoParamRAOAlgorithmFactory::createRAOAlgorithm(): unknown type of algorithm");
    }
  }
  return ret;
}

} // namespace castor::tape::tapeserver::rao
