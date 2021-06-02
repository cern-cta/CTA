/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "RAOAlgorithmFactory.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * Factory of EnterpriseRAOAlgorithm. 
 */
class EnterpriseRAOAlgorithmFactory : public RAOAlgorithmFactory{
public:
  /**
   * Constructor of this factory
   * @param drive the DriveInterface to perform a RAO query 
   * @param maxFilesSupported the maximum number of files the drive supports to perform a RAO query
   */
  EnterpriseRAOAlgorithmFactory(castor::tape::tapeserver::drive::DriveInterface * drive, const uint64_t maxFilesSupported);
  
  /**
   * Returns an Enteprise RAO Algorithm
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;
  virtual ~EnterpriseRAOAlgorithmFactory();
private:
  drive::DriveInterface * m_drive;
  uint64_t m_maxFilesSupported;
};

}}}}