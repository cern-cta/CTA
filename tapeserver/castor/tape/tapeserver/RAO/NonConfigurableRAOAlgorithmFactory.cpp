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

#include "tapeserver/castor/tape/tapeserver/RAO/NonConfigurableRAOAlgorithmFactory.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/LinearRAOAlgorithm.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/RandomRAOAlgorithm.hpp"

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
