/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigurableRAOAlgorithmFactory.hpp"

#include "SLTFRAOAlgorithm.hpp"

namespace castor::tape::tapeserver::rao {

ConfigurableRAOAlgorithmFactory::ConfigurableRAOAlgorithmFactory(const RAOParams& raoParams) : m_raoParams(raoParams) {}

std::unique_ptr<RAOAlgorithm> ConfigurableRAOAlgorithmFactory::createRAOAlgorithm() {
  std::unique_ptr<RAOAlgorithm> ret;
  switch (m_raoParams.getAlgorithmType()) {
    case RAOParams::RAOAlgorithmType::sltf: {
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

ConfigurableRAOAlgorithmFactory::Builder::Builder(const RAOParams& raoParams) {
  m_configurableRAOAlgoFactory.reset(new ConfigurableRAOAlgorithmFactory(raoParams));
}

void ConfigurableRAOAlgorithmFactory::Builder::setCatalogue(cta::catalogue::Catalogue* catalogue) {
  m_configurableRAOAlgoFactory->m_catalogue = catalogue;
}

void ConfigurableRAOAlgorithmFactory::Builder::setDriveInterface(drive::DriveInterface* drive) {
  m_configurableRAOAlgoFactory->m_drive = drive;
}

std::unique_ptr<ConfigurableRAOAlgorithmFactory> ConfigurableRAOAlgorithmFactory::Builder::build() {
  return std::move(m_configurableRAOAlgoFactory);
}

}  // namespace castor::tape::tapeserver::rao
