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

#pragma once

#include "RAOParams.hpp"
#include "RAOAlgorithmFactory.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * This factory allows to instanciate RAO algorithm that do not need any
 * parameter to work. E.G the linear algorithm just does a sort of the fseqs,
 * it does not need any parameter.
 */
class NonConfigurableRAOAlgorithmFactory : public RAOAlgorithmFactory {
public:
  /**
   * Constructor
   *
   * @param type    used by the createRAOAlgorithm() method to instantiate the correct algorithm for the type
   */
  explicit NonConfigurableRAOAlgorithmFactory(const RAOParams::RAOAlgorithmType& type) : m_type(type) {}

  ~NonConfigurableRAOAlgorithmFactory() final = default;

  /**
   * Returns the correct instance of RAO algorithm regarding the type
   * given while constructing this factory.
   * @throws Exception if the type is unknown
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;

private:
  RAOParams::RAOAlgorithmType m_type;
};

}  // namespace castor::tape::tapeserver::rao
