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

#include "RAOAlgorithmFactory.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::rao {

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
  EnterpriseRAOAlgorithmFactory(drive::DriveInterface* drive, const uint64_t maxFilesSupported) :
    m_drive(drive), m_maxFilesSupported(maxFilesSupported) { }

  virtual ~EnterpriseRAOAlgorithmFactory() = default;
  
  /**
   * Returns an Enteprise RAO Algorithm
   */
  std::unique_ptr<RAOAlgorithm> createRAOAlgorithm() override;

private:
  drive::DriveInterface * m_drive;
  uint64_t m_maxFilesSupported;
};

} // namespace castor::tape::tapeserver::rao
