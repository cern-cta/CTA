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

#include "RAOAlgorithm.hpp"
#include "NonConfigurableRAOAlgorithmFactory.hpp"


namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
/**
 * This class represents a LinearRAOAlgorithm 
 */
class LinearRAOAlgorithm : public RAOAlgorithm {
public:
  LinearRAOAlgorithm();
  /**
   * This method will return the indexes of the jobs that are reoreded in a linear way (sorted by fseq ascendant)
   * Example : if the fseqs of jobs in parameter are arranged like this [2, 3, 1, 4] the 
   * algorithm will return the following indexes vector : [2, 0, 1, 3]
   * @param jobs the jobs to perform the linear RAO query
   * @return the indexes of the jobs ordered by fseq ascendant
   */
  std::vector<uint64_t> performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) override;
  virtual ~LinearRAOAlgorithm();
  
  std::string getName() const override;
};

}}}}