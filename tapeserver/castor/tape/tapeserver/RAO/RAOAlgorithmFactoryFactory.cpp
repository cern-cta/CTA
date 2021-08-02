/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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


#include "RAOAlgorithmFactoryFactory.hpp"
#include "common/log/LogContext.hpp"
#include "EnterpriseRAOAlgorithmFactory.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"
#include "ConfigurableRAOAlgorithmFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

RAOAlgorithmFactoryFactory::RAOAlgorithmFactoryFactory(RAOManager & raoManager, cta::log::LogContext & lc):m_raoManager(raoManager), m_lc(lc) {
}

std::unique_ptr<RAOAlgorithmFactory> RAOAlgorithmFactoryFactory::createAlgorithmFactory() {
  std::unique_ptr<RAOAlgorithmFactory> ret;
  auto maxFilesSupported = m_raoManager.getMaxFilesSupported();
  if(m_raoManager.isDriveEnterpriseEnabled()) {
    if(m_raoManager.hasUDS() && maxFilesSupported){
      //We successfully queried the max limits UDS of the drive, we then return an Enterprise RAO factory
      ret.reset(new EnterpriseRAOAlgorithmFactory(m_raoManager.getDrive(),maxFilesSupported.value()));
    } 
  } else {
    RAOParams raoParams = m_raoManager.getRAOParams();
    //We will instanciate a CTA RAO algorithm
    RAOParams::RAOAlgorithmType raoAlgoType;
    try {
      raoAlgoType = raoParams.getAlgorithmType();
    } catch (const cta::exception::Exception & ex) {
      //We failed to determine the RAOAlgorithmType, we use the linear algorithm by default
      //log a warning
      std::string msg = "In RAOAlgorithmFactoryFactory::createAlgorithmFactory(), unable to determine the RAO algorithm to use, the algorithm name provided"
        " in the tapeserver configuration is " + raoParams.getRAOAlgorithmName() + " the available algorithm names are " + raoParams.getCTARAOAlgorithmNameAvailable()
        + ". We will apply a linear algorithm instead.";
      m_lc.log(cta::log::WARNING, msg);
      raoAlgoType = RAOParams::RAOAlgorithmType::linear;
    }
    switch(raoAlgoType){
      case RAOParams::RAOAlgorithmType::linear:
      case RAOParams::RAOAlgorithmType::random:
      {
        ret.reset(new NonConfigurableRAOAlgorithmFactory(raoAlgoType));
        break;
      }
      case RAOParams::RAOAlgorithmType::sltf:
      { 
        ConfigurableRAOAlgorithmFactory::Builder builder(raoParams);
        builder.setDriveInterface(m_raoManager.getDrive());
        builder.setCatalogue(m_raoManager.getCatalogue());
        ret = builder.build();
        break;
      }
      default:
        break;
    }
  }
  return ret;
}


RAOAlgorithmFactoryFactory::~RAOAlgorithmFactoryFactory() {
}

}}}}