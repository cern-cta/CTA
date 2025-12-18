/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RAOAlgorithm.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * Abstract class that will be extended by subclasses in order
 * to instanciate an RAOAlgorithm
 */
class RAOAlgorithmFactory {
public:
  virtual ~RAOAlgorithmFactory() = default;

  /**
   * Creates an RAO algorithm that will be used to do a Recommended Access Order
   * in order to give an optimized order to Retrieve files efficiently from a tape
   * @return the RAO algorithm instance
   */
  virtual std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() = 0;

  std::unique_ptr<RAOAlgorithm> createDefaultLinearAlgorithm();
};

}  // namespace castor::tape::tapeserver::rao
