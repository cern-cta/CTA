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

#include "NonConfigurableRAOAlgorithmFactory.hpp"
#include "LinearRAOAlgorithm.hpp"
#include "RandomRAOAlgorithm.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

NonConfigurableRAOAlgorithmFactory::NonConfigurableRAOAlgorithmFactory(const RAOParams::RAOAlgorithmType & type) : m_type(type) {
}

NonConfigurableRAOAlgorithmFactory::~NonConfigurableRAOAlgorithmFactory() {
}

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


}}}}