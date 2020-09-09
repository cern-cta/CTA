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
      checkDriveInterfaceSet();
      checkCatalogueSet();
      SLTFRAOAlgorithm::Builder builder(m_raoParams,m_drive,m_catalogue);
      ret = builder.build();
      break;
    }
    default:
      throw cta::exception::Exception("Unknown type of ConfigurableRAOAlgorithm. Existing types are : sltf");
      break;
  }
  return ret;
}

void ConfigurableRAOAlgorithmFactory::checkDriveInterfaceSet() const {
  if(m_drive == nullptr){
    throw cta::exception::Exception("In ConfigurableRAOAlgorithmFactory::checkDriveInterfaceSet(), the drive interface has not been set");
  }
}

void ConfigurableRAOAlgorithmFactory::checkCatalogueSet() const {
  if(m_catalogue == nullptr){
    throw cta::exception::Exception("In ConfigurableRAOAlgorithmFactory::checkCatalogueSet(), the catalogue has not been set.");
  }
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