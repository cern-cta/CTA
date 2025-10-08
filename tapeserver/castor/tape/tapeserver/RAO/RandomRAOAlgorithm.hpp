/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "RAOAlgorithm.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"

namespace castor::tape::tapeserver::rao {

class NonConfigurableRAOAlgorithmFactory;

/**
 * This RAO Algorithm is a random one. The indexes of the jobs passed in parameter
 * will be organized randomly
 */
class RandomRAOAlgorithm : public RAOAlgorithm{
public:
  friend NonConfigurableRAOAlgorithmFactory;

  ~RandomRAOAlgorithm() final = default;

  /**
   * Returns a randomly organized vector of the indexes of the jobs passed in parameter
   * @param jobs the jobs to perform the random RAO on
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;
  std::string getName() const override { return "random"; }

private:
  RandomRAOAlgorithm() = default;
};

} // namespace castor::tape::tapeserver::rao
