/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  explicit NonConfigurableRAOAlgorithmFactory(const RAOParams::RAOAlgorithmType& type) :
    m_type(type) { }

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

} // namespace castor::tape::tapeserver::rao
