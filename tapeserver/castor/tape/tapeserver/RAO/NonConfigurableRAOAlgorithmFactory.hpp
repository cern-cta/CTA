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

#include "RAOConfigurationData.hpp"
#include "RAOAlgorithmFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
/**
 * This factory allows to instanciate RAO algorithm that do not need any
 * parameter to work. E.G the linear algorithm just does a sort of the fseqs,
 * it does not need any parameter.
 */
class NonConfigurableRAOAlgorithmFactory : public RAOAlgorithmFactory {
public:
  /**
   * Constructor
   * @param type the type given will be used by the createRAOAlgorithm() method
   * to instanciate the correct algorithm regarding its type
   */
  NonConfigurableRAOAlgorithmFactory(const RAOConfigurationData::RAOAlgorithmType & type);
  /**
   * Returns the correct instance of RAO algorithm regarding the type
   * given while constructing this factory.
   * @throws Exception if the type is unknown
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;
  virtual ~NonConfigurableRAOAlgorithmFactory();
private:
  RAOConfigurationData::RAOAlgorithmType m_type;
};

}}}}