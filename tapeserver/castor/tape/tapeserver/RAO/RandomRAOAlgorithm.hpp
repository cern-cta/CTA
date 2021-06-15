/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "RAOAlgorithm.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class NonConfigurableRAOAlgorithmFactory;

/**
 * This RAO Algorithm is a random one. The indexes of the jobs passed in parameter
 * will be organized randomly 
 */
class RandomRAOAlgorithm : public RAOAlgorithm{
public:
  friend NonConfigurableRAOAlgorithmFactory;
  /**
   * Returns a randomly organized vector of the indexes of the jobs passed in parameter
   * @param jobs the jobs to perform the random RAO on
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;
  std::string getName() const override;
  virtual ~RandomRAOAlgorithm();
private:
  RandomRAOAlgorithm();
};

}}}}