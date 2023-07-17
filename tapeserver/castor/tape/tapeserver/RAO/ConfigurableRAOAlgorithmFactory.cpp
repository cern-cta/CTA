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


#include "ConfigurableRAOAlgorithmFactory.hpp"
#include "SLTFRAOAlgorithm.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

ConfigurableRAOAlgorithmFactory::ConfigurableRAOAlgorithmFactory(const RAOParams & raoParams):m_raoParams(raoParams){}

ConfigurableRAOAlgorithmFactory::~ConfigurableRAOAlgorithmFactory() {
}

std::unique_ptr<RAOAlgorithm> ConfigurableRAOAlgorithmFactory::createRAOAlgorithm() {
  std::unique_ptr<RAOAlgorithm> ret;
  switch(m_raoParams.getAlgorithmType()){
    case RAOParams::RAOAlgorithmType::sltf:{
      SLTFRAOAlgorithm::Builder builder(m_raoParams);
      builder.setCatalogue(m_catalogue);
      builder.setDrive(m_drive);
      ret = builder.build();
      break;
    }
    default:
      throw cta::exception::Exception("Unknown type of ConfigurableRAOAlgorithm. Existing types are : sltf");
  }
  return ret;
}

ConfigurableRAOAlgorithmFactory::Builder::Builder(const RAOParams& raoParams){
  m_configurableRAOAlgoFactory.reset(new ConfigurableRAOAlgorithmFactory(raoParams));
}

void ConfigurableRAOAlgorithmFactory::Builder::setCatalogue(cta::catalogue::Catalogue * catalogue) {
  m_configurableRAOAlgoFactory->m_catalogue = catalogue;
}

void ConfigurableRAOAlgorithmFactory::Builder::setDriveInterface(drive::DriveInterface* drive) {
  m_configurableRAOAlgoFactory->m_drive = drive;
}

std::unique_ptr<ConfigurableRAOAlgorithmFactory> ConfigurableRAOAlgorithmFactory::Builder::build(){
  return std::move(m_configurableRAOAlgoFactory);
}

}}}}
