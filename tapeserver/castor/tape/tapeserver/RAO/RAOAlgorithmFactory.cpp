/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include "RAOAlgorithmFactory.hpp"
#include "LinearRAOAlgorithm.hpp"

namespace castor::tape::tapeserver::rao {

std::unique_ptr<RAOAlgorithm> RAOAlgorithmFactory::createDefaultLinearAlgorithm() {
  std::unique_ptr<RAOAlgorithm> ret;
  ret.reset(new LinearRAOAlgorithm());
  return ret;
}

} // namespace castor::tape::tapeserver::rao
