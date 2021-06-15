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

namespace castor { namespace tape { namespace tapeserver { namespace rao {

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

}}}}