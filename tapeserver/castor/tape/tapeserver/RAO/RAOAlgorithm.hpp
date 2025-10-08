/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <vector>
#include <memory>
#include "scheduler/RetrieveJob.hpp"
#include "common/log/TimingList.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * Abstract class that represents an RAO algorithm
 */
class RAOAlgorithm {
protected:
  //Timing list to store the timings of the different steps of the RAO algorithm
  cta::log::TimingList m_raoTimings;
public:
  virtual ~RAOAlgorithm() = default;

  /**
   * Returns the vector of indexes of the jobs passed in parameter
   * sorted according to an algorithm
   * @param jobs the jobs to perform RAO on
   * @return the vector of indexes sorted by an algorithm applied on the jobs passed in parameter
   */
  virtual std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>> & jobs) = 0;

  /**
   * Returns the timings the RAO Algorithm took to perform each step
   * @return the timings the RAO Algorithm took to perform each step
   */
  cta::log::TimingList getRAOTimings() const { return m_raoTimings; }

  /**
   * Returns the name of the RAO algorithm
   * @return the name of the RAO algorithm
   */
  virtual std::string getName() const = 0;
};

} // namespace castor::tape::tapeserver::rao
