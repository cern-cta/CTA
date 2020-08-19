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

ConfigurableRAOAlgorithmFactory::ConfigurableRAOAlgorithmFactory(drive::DriveInterface * drive, cta::catalogue::Catalogue * catalogue, const RAOConfigurationData & raoConfigurationData):m_drive(drive),
    m_catalogue(catalogue), m_raoConfigurationData(raoConfigurationData){}

ConfigurableRAOAlgorithmFactory::~ConfigurableRAOAlgorithmFactory() {
}

std::unique_ptr<RAOAlgorithm> ConfigurableRAOAlgorithmFactory::createRAOAlgorithm() {
  std::unique_ptr<RAOAlgorithm> ret;
  switch(m_raoConfigurationData.getAlgorithmType()){
    case RAOConfigurationData::RAOAlgorithmType::sltf:{
      SLTFRAOAlgorithm::Builder builder(m_raoConfigurationData,m_drive,m_catalogue);
      ret.reset(builder.build().release());
      break;
    }
    default:
      throw cta::exception::Exception("Unknown type of ConfigurableRAOAlgorithm. Existing types are : sltf");
      break;
  }
  return ret;
}


}}}}