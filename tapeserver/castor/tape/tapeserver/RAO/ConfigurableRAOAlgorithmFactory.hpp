/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

#include "tapeserver/castor/tape/tapeserver/RAO/RAOParams.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/RAOAlgorithmFactory.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace cta::catalogue {
class Catalogue;
}

namespace castor::tape::tapeserver::rao {

/**
 * This factory instanciates a configurable RAO algorithm
 * @param raoParams the parameters to configure the RAO algorithm
 */
class ConfigurableRAOAlgorithmFactory : public RAOAlgorithmFactory {
public:
  ~ConfigurableRAOAlgorithmFactory() final = default;

  /**
   * Instanciates a configurable RAO algorithm
   * @return a unique_ptr to a configurable RAO algorithm
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;

  /**
   * Builder class to build the ConfigurableRAOAlgorithmFactory
   * @param raoParams the parameters of the Configurable RAOAlgorithm that will be instanciated by this factory
   */
  class Builder {
  public:
    explicit Builder(const RAOParams & raoParams);
    /**
     * If the factory need to talk to the drive before instanciating
     * the RAOAlgorithm, the drive interface should be given by using this method.
     * @param drive the drive interface to talk to the drive if necessary
     */
    void setDriveInterface(drive::DriveInterface * drive);
    /**
     * If the factory need to talk to the catalogue before instanciating the RAOAlgorithm,
     * the catalogue should be given by using this method
     * @param catalogue the catalogue to talk to it
     */
    void setCatalogue(cta::catalogue::Catalogue * catalogue);
    /**
     * Returns the unique pointer to instance of a ConfigurableRAOAlgorithmFactory
     * @return the unique pointer to instance of a ConfigurableRAOAlgorithmFactory
     */
    std::unique_ptr<ConfigurableRAOAlgorithmFactory> build();
  private:
    std::unique_ptr<ConfigurableRAOAlgorithmFactory> m_configurableRAOAlgoFactory;
  };

private:
  explicit ConfigurableRAOAlgorithmFactory(const RAOParams & raoParams);
  drive::DriveInterface * m_drive = nullptr;
  cta::catalogue::Catalogue * m_catalogue = nullptr;
  RAOParams m_raoParams;
};

} // namespace castor::tape::tapeserver::rao
