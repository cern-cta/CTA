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

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

/**
 * Abstract class that will be extended by subclasses in order
 * to instanciate an RAOAlgorithm
 */
class RAOAlgorithmFactory {
public:
  /**
   * Creates an RAO algorithm that will be used to do a Recommended Access Order
   * in order to give an optimized order to Retrieve files efficiently from a tape
   * @return the RAO algorithm instance
   */
  virtual std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() = 0;

  std::unique_ptr<RAOAlgorithm> createDefaultLinearAlgorithm();

  virtual ~RAOAlgorithmFactory();

private:
};

}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor