/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RAOAlgorithm.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"


namespace castor::tape::tapeserver::rao {

/**
 * This class represents a LinearRAOAlgorithm
 */
class LinearRAOAlgorithm : public RAOAlgorithm {
public:
  LinearRAOAlgorithm() = default;
  ~LinearRAOAlgorithm() final = default;

  /**
   * This method will return the indexes of the jobs that are reoreded in a linear way (sorted by fseq ascendant)
   * Example : if the fseqs of jobs in parameter are arranged like this [2, 3, 1, 4] the
   * algorithm will return the following indexes vector : [2, 0, 1, 3]
   * @param jobs the jobs to perform the linear RAO query
   * @return the indexes of the jobs ordered by fseq ascendant
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;

  std::string getName() const override;
};

} // namespace castor::tape::tapeserver::rao
