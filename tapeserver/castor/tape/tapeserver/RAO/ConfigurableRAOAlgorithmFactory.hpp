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

#pragma once

#include "RAOParams.hpp"
#include "RAOAlgorithmFactory.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/Catalogue.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * This factory instanciates a configurable RAO algorithm
 * @param raoParams the parameters to configure the RAO algorithm
 */
class ConfigurableRAOAlgorithmFactory : public RAOAlgorithmFactory{
public:

  /**
   * Instanciates a configurable RAO algorithm
   * @return a unique_ptr to a configurable RAO algorithm
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;
  virtual ~ConfigurableRAOAlgorithmFactory();
  
  /**
   * Builder class to build the ConfigurableRAOAlgorithmFactory
   * @param raoParams the parameters of the Configurable RAOAlgorithm that will be instanciated by this factory
   */
  class Builder {
  public:
    Builder(const RAOParams & raoParams);
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
  ConfigurableRAOAlgorithmFactory(const RAOParams & raoParams);
  drive::DriveInterface * m_drive = nullptr;
  cta::catalogue::Catalogue * m_catalogue = nullptr;
  RAOParams m_raoParams;
};

}}}}

